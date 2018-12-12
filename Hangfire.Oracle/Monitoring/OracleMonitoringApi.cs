using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

using Dapper;

using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.Oracle.Core.Entities;
using Hangfire.Oracle.Core.JobQueue;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

namespace Hangfire.Oracle.Core.Monitoring
{
    internal class OracleMonitoringApi : IMonitoringApi
    {
        private readonly OracleStorage _storage;
        private readonly int? _jobListLimit;

        public OracleMonitoringApi([NotNull] OracleStorage storage, int? jobListLimit)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _jobListLimit = jobListLimit;
        }

        public IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            var tuples = _storage.QueueProviders
                .Select(x => x.GetJobQueueMonitoringApi())
                .SelectMany(x => x.GetQueues(), (monitoring, queue) => new { Monitoring = monitoring, Queue = queue })
                .OrderBy(x => x.Queue)
                .ToArray();

            var result = new List<QueueWithTopEnqueuedJobsDto>(tuples.Length);

            foreach (var tuple in tuples)
            {
                var enqueuedJobIds = tuple.Monitoring.GetEnqueuedJobIds(tuple.Queue, 0, 5);
                var counters = tuple.Monitoring.GetEnqueuedAndFetchedCount(tuple.Queue);

                var firstJobs = UseConnection(connection => EnqueuedJobs(connection, enqueuedJobIds));

                result.Add(new QueueWithTopEnqueuedJobsDto
                {
                    Name = tuple.Queue,
                    Length = counters.EnqueuedCount ?? 0,
                    Fetched = counters.FetchedCount,
                    FirstJobs = firstJobs
                });
            }

            return result;
        }

        public IList<ServerDto> Servers()
        {
            return UseConnection<IList<ServerDto>>(connection =>
            {
                var servers =
                    connection.Query<Entities.Server>("SELECT * FROM MISP.HF_SERVER").ToList();

                var result = new List<ServerDto>();

                foreach (var server in servers)
                {
                    var data = JobHelper.FromJson<ServerData>(server.Data);
                    result.Add(new ServerDto
                    {
                        Name = server.Id,
                        Heartbeat = server.LastHeartbeat,
                        Queues = data.Queues,
                        StartedAt = data.StartedAt ?? DateTime.MinValue,
                        WorkersCount = data.WorkerCount
                    });
                }

                return result;
            });
        }

        public JobDetailsDto JobDetails(string jobId)
        {
            return UseConnection(connection =>
            {
                const string sql = @"
 SELECT ID as Id, STATE_ID as StateId, STATE_NAME as StateName, INVOCATION_DATA as InvocationData, ARGUMENTS as Arguments, CREATED_AT as CreatedAt, EXPIRE_AT as ExpireAt FROM MISP.HF_JOB WHERE ID = :ID;
 SELECT ID as Id, NAME as Name, VALUE as Value, JOB_ID as JobId FROM MISP.HF_JOB_PARAMETER WHERE JOB_ID = :ID;
 SELECT ID as Id, JOB_ID as JobId, NAME as Name, REASON as Reason, CREATED_AT as CreatedAt, DATA as Data FROM MISP.HF_JOB_STATE where JOB_ID = :ID ORDER BY ID DESC;";

                using (var multi = connection.QueryMultiple(sql, new { ID = jobId }))
                {
                    var job = multi.Read<SqlJob>().SingleOrDefault();
                    if (job == null)
                    {
                        return null;
                    }

                    var parameters = multi.Read<JobParameter>().ToDictionary(x => x.Name, x => x.Value);
                    var history =
                        multi.Read<SqlState>()
                            .ToList()
                            .Select(x => new StateHistoryDto
                            {
                                StateName = x.Name,
                                CreatedAt = x.CreatedAt,
                                Reason = x.Reason,
                                Data = new Dictionary<string, string>(JobHelper.FromJson<Dictionary<string, string>>(x.Data), StringComparer.OrdinalIgnoreCase),
                            })
                            .ToList();

                    return new JobDetailsDto
                    {
                        CreatedAt = job.CreatedAt,
                        ExpireAt = job.ExpireAt,
                        Job = DeserializeJob(job.InvocationData, job.Arguments),
                        History = history,
                        Properties = parameters
                    };
                }
            });
        }

        public StatisticsDto GetStatistics()
        {
            const string jobQuery = "SELECT COUNT(ID) FROM MISP.HF_JOB WHERE STATE_NAME = :STATE_NAME";
            const string succeededQuery = @"
 SELECT SUM(S.VALUE) FROM (
     SELECT SUM(VALUE) AS VALUE FROM MISP.HF_COUNTER WHERE KEY = :KEY
     UNION ALL
     SELECT VALUE FROM MISP.HF_AGGREGATED_COUNTER WHERE KEY = :KEY) AS S
";

            var statistics =
                UseConnection(connection =>
                    new StatisticsDto
                    {
                        Enqueued = connection.ExecuteScalar<int>(jobQuery, new { STATE_NAME = "Enqueued" }),
                        Failed = connection.ExecuteScalar<int>(jobQuery, new { STATE_NAME = "Failed" }),
                        Processing = connection.ExecuteScalar<int>(jobQuery, new { STATE_NAME = "Processing" }),
                        Scheduled = connection.ExecuteScalar<int>(jobQuery, new { STATE_NAME = "Scheduled" }),
                        Servers = connection.ExecuteScalar<int>("SELECT COUNT(ID) FROM MISP.HF_SERVER"),
                        Succeeded = connection.ExecuteScalar<int>(succeededQuery, new { KEY = "stats:succeeded" }),
                        Deleted = connection.ExecuteScalar<int>(succeededQuery, new { KEY = "stats:deleted" }),
                        Recurring =
                            connection.ExecuteScalar<int>("SELECT COUNT(*) FROM MISP.HF_SET where KEY = 'recurring-jobs'")
                    });

            statistics.Queues = _storage.QueueProviders
                .SelectMany(x => x.GetJobQueueMonitoringApi().GetQueues())
                .Count();

            return statistics;
        }

        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int @from, int perPage)
        {
            var queueApi = GetQueueApi(queue);
            var enqueuedJobIds = queueApi.GetEnqueuedJobIds(queue, from, perPage);

            return UseConnection(connection => EnqueuedJobs(connection, enqueuedJobIds));
        }

        public JobList<FetchedJobDto> FetchedJobs(string queue, int @from, int perPage)
        {
            var queueApi = GetQueueApi(queue);
            var fetchedJobIds = queueApi.GetFetchedJobIds(queue, from, perPage);

            return UseConnection(connection => FetchedJobs(connection, fetchedJobIds));
        }

        public JobList<ProcessingJobDto> ProcessingJobs(int @from, int count)
        {
            return UseConnection(connection => GetJobs(
                connection,
                from, count,
                ProcessingState.StateName,
                (sqlJob, job, stateData) => new ProcessingJobDto
                {
                    Job = job,
                    ServerId = stateData.ContainsKey("ServerId") ? stateData["ServerId"] : stateData["ServerName"],
                    StartedAt = JobHelper.DeserializeDateTime(stateData["StartedAt"]),
                }));
        }

        public JobList<ScheduledJobDto> ScheduledJobs(int @from, int count)
        {
            return UseConnection(connection => GetJobs(
                connection,
                from, count,
                ScheduledState.StateName,
                (sqlJob, job, stateData) => new ScheduledJobDto
                {
                    Job = job,
                    EnqueueAt = JobHelper.DeserializeDateTime(stateData["EnqueueAt"]),
                    ScheduledAt = JobHelper.DeserializeDateTime(stateData["ScheduledAt"])
                }));
        }

        public JobList<SucceededJobDto> SucceededJobs(int @from, int count)
        {
            return UseConnection(connection => GetJobs(
                connection,
                from,
                count,
                SucceededState.StateName,
                (sqlJob, job, stateData) => new SucceededJobDto
                {
                    Job = job,
                    Result = stateData.ContainsKey("Result") ? stateData["Result"] : null,
                    TotalDuration = stateData.ContainsKey("PerformanceDuration") && stateData.ContainsKey("Latency")
                        ? (long?)long.Parse(stateData["PerformanceDuration"]) + (long?)long.Parse(stateData["Latency"])
                        : null,
                    SucceededAt = JobHelper.DeserializeNullableDateTime(stateData["SucceededAt"])
                }));
        }

        public JobList<FailedJobDto> FailedJobs(int @from, int count)
        {
            return UseConnection(connection => GetJobs(
                connection,
                from,
                count,
                FailedState.StateName,
                (sqlJob, job, stateData) => new FailedJobDto
                {
                    Job = job,
                    Reason = sqlJob.StateReason,
                    ExceptionDetails = stateData["ExceptionDetails"],
                    ExceptionMessage = stateData["ExceptionMessage"],
                    ExceptionType = stateData["ExceptionType"],
                    FailedAt = JobHelper.DeserializeNullableDateTime(stateData["FailedAt"])
                }));
        }

        public JobList<DeletedJobDto> DeletedJobs(int @from, int count)
        {
            return UseConnection(connection => GetJobs(
                connection,
                from,
                count,
                DeletedState.StateName,
                (sqlJob, job, stateData) => new DeletedJobDto
                {
                    Job = job,
                    DeletedAt = JobHelper.DeserializeNullableDateTime(stateData["DeletedAt"])
                }));
        }

        public long ScheduledCount()
        {
            return UseConnection(connection =>
                GetNumberOfJobsByStateName(connection, ScheduledState.StateName));
        }

        public long EnqueuedCount(string queue)
        {
            var queueApi = GetQueueApi(queue);
            var counters = queueApi.GetEnqueuedAndFetchedCount(queue);

            return counters.EnqueuedCount ?? 0;
        }

        public long FetchedCount(string queue)
        {
            var queueApi = GetQueueApi(queue);
            var counters = queueApi.GetEnqueuedAndFetchedCount(queue);

            return counters.FetchedCount ?? 0;
        }

        public long FailedCount()
        {
            return UseConnection(connection =>
                GetNumberOfJobsByStateName(connection, FailedState.StateName));
        }

        public long ProcessingCount()
        {
            return UseConnection(connection =>
                GetNumberOfJobsByStateName(connection, ProcessingState.StateName));
        }

        public long SucceededListCount()
        {
            return UseConnection(connection =>
                GetNumberOfJobsByStateName(connection, SucceededState.StateName));
        }

        public long DeletedListCount()
        {
            return UseConnection(connection =>
                GetNumberOfJobsByStateName(connection, DeletedState.StateName));
        }

        public IDictionary<DateTime, long> SucceededByDatesCount()
        {
            return UseConnection(connection =>
                GetTimelineStats(connection, "succeeded"));
        }

        public IDictionary<DateTime, long> FailedByDatesCount()
        {
            return UseConnection(connection =>
                GetTimelineStats(connection, "failed"));
        }

        public IDictionary<DateTime, long> HourlySucceededJobs()
        {
            return UseConnection(connection =>
                GetHourlyTimelineStats(connection, "succeeded"));
        }

        public IDictionary<DateTime, long> HourlyFailedJobs()
        {
            return UseConnection(connection =>
                GetHourlyTimelineStats(connection, "failed"));
        }

        private T UseConnection<T>(Func<IDbConnection, T> action)
        {
            return _storage.UseTransaction(action, IsolationLevel.ReadUncommitted);
        }

        private long GetNumberOfJobsByStateName(IDbConnection connection, string stateName)
        {
            var sqlQuery = _jobListLimit.HasValue
                ? "SELECT COUNT(J.ID) FROM (SELECT ID FROM MISP.HF_JOB WHERE STATE_NAME = :STATE_NAME AND ROWNUM <= :LIMIT) AS J"
                : "SELECT COUNT(ID) FROM MISP.HF_JOB WHERE STATE_NAME = :STATE_NAME";

            var count = connection.QuerySingle<int>(
                 sqlQuery,
                 new { STATE_NAME = stateName, LIMIT = _jobListLimit });

            return count;
        }
        private IPersistentJobQueueMonitoringApi GetQueueApi(string queueName)
        {
            var provider = _storage.QueueProviders.GetProvider(queueName);
            var monitoringApi = provider.GetJobQueueMonitoringApi();

            return monitoringApi;
        }

        private JobList<TDto> GetJobs<TDto>(IDbConnection connection, int from, int count, string stateName,
            Func<SqlJob, Job, Dictionary<string, string>, TDto> selector)
        {
            // TODO: QUERY fix
            const string jobsSql = @"SELECT * FROM (
  SELECT J.ID AS Id, J.STATE_ID AS StateId, J.STATE_NAME AS StateName, J.INVOCATION_DATA AS InvocationData, J.ARGUMENTS AS Arguments, J.CREATED_AT AS CreatedAt, J.EXPIRE_AT AS ExpireAt, S.REASON AS StateReason, S.DATA AS StateData , s.Reason as StateReason, s.Data as StateData, @rownum := @rownum + 1 AS rankvalue
    FROM MISP.HF_JOB J
    cross join (SELECT @rownum := 0) r
  left join State s on j.StateId = s.Id
  where j.StateName = :STATE_NAME
  order by j.Id desc
) as j where j.rankvalue between :START and :END ";

            var jobs =
                connection.Query<SqlJob>(
                    jobsSql,
                    new { STATE_NAME = stateName, START = @from + 1, END = @from + count })
                    .ToList();

            return DeserializeJobs(jobs, selector);
        }

        private static JobList<TDto> DeserializeJobs<TDto>(
            ICollection<SqlJob> jobs,
            Func<SqlJob, Job, Dictionary<string, string>, TDto> selector)
        {
            var result = new List<KeyValuePair<string, TDto>>(jobs.Count);

            foreach (var job in jobs)
            {
                var deserializedData = JobHelper.FromJson<Dictionary<string, string>>(job.StateData);
                var stateData = deserializedData != null
                    ? new Dictionary<string, string>(deserializedData, StringComparer.OrdinalIgnoreCase)
                    : null;

                var dto = selector(job, DeserializeJob(job.InvocationData, job.Arguments), stateData);

                result.Add(new KeyValuePair<string, TDto>(
                    job.Id.ToString(), dto));
            }

            return new JobList<TDto>(result);
        }

        private static Job DeserializeJob(string invocationData, string arguments)
        {
            var data = JobHelper.FromJson<InvocationData>(invocationData);
            data.Arguments = arguments;

            try
            {
                return data.Deserialize();
            }
            catch (JobLoadException)
            {
                return null;
            }
        }

        private Dictionary<DateTime, long> GetTimelineStats(
            IDbConnection connection,
            string type)
        {
            var endDate = DateTime.UtcNow.Date;
            var dates = new List<DateTime>();
            for (var i = 0; i < 7; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddDays(-1);
            }

            var keyMaps = dates.ToDictionary(x => $"stats:{type}:{x:yyyy-MM-dd}", x => x);

            return GetTimelineStats(connection, keyMaps);
        }

        private Dictionary<DateTime, long> GetTimelineStats(IDbConnection connection, IDictionary<string, DateTime> keyMaps)
        {
            var valuesMap = connection.Query(
                "SELECT KEY AS Key, VALUE AS Count FROM MISP.HF_AGGREGATED_COUNTER WHERE KEY in :KEYS",
                new { KEYS = keyMaps.Keys })
                .ToDictionary(x => (string)x.Key, x => (long)x.Count);

            foreach (var key in keyMaps.Keys)
            {
                if (!valuesMap.ContainsKey(key)) valuesMap.Add(key, 0);
            }

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < keyMaps.Count; i++)
            {
                var value = valuesMap[keyMaps.ElementAt(i).Key];
                result.Add(keyMaps.ElementAt(i).Value, value);
            }

            return result;
        }

        private JobList<EnqueuedJobDto> EnqueuedJobs(IDbConnection connection, IEnumerable<int> jobIds)
        {
            var enumerable = jobIds as int[] ?? jobIds.ToArray();
            var enqueuedJobsSql = @"
 SELECT J.ID AS Id, J.STATE_ID AS StateId, J.STATE_NAME AS StateName, J.INVOCATION_DATA AS InvocationData, J.ARGUMENTS AS Arguments, J.CREATED_AT AS CreatedAt, J.EXPIRE_AT AS ExpireAt, S.REASON AS StateReason, S.DATA AS StateData
   FROM MISP.HF_JOB J
 LEFT JOIN MISP.HF_JOB_STATE S 
     ON S.ID = J.STATE_ID
  WHERE J.ID in :JOB_IDS";

            if (!enumerable.Any())
            {
                enqueuedJobsSql = @"
 SELECT J.ID AS Id, J.STATE_ID AS StateId, J.STATE_NAME AS StateName, J.INVOCATION_DATA AS InvocationData, J.ARGUMENTS AS Arguments, J.CREATED_AT AS CreatedAt, J.EXPIRE_AT AS ExpireAt, S.REASON AS StateReason, S.DATA AS StateData
   FROM MISP.HF_JOB J
 LEFT JOIN MISP.HF_JOB_STATE S 
     ON S.ID = J.STATE_ID
";
            }
            var jobs = connection.Query<SqlJob>(
                enqueuedJobsSql,
                new { jobIds = enumerable })
                .ToList();

            return DeserializeJobs(
                jobs,
                (sqlJob, job, stateData) => new EnqueuedJobDto
                {
                    Job = job,
                    State = sqlJob.StateName,
                    EnqueuedAt = sqlJob.StateName == EnqueuedState.StateName
                        ? JobHelper.DeserializeNullableDateTime(stateData["EnqueuedAt"])
                        : null
                });
        }

        private JobList<FetchedJobDto> FetchedJobs(IDbConnection connection, IEnumerable<int> jobIds)
        {
            const string fetchedJobsSql = @"
 SELECT J.ID AS Id, J.STATE_ID AS StateId, J.STATE_NAME AS StateName, J.INVOCATION_DATA AS InvocationData, J.ARGUMENTS AS Arguments, J.CREATED_AT AS CreatedAt, J.EXPIRE_AT AS ExpireAt, S.REASON AS StateReason, S.DATA AS StateData 
   FROM MISP.HF_JOB J
 LEFT JOIN MISP.HF_JOB_STATE S ON S.ID = J.STATE_ID
  WHERE J.ID IN :JOB_IDS";

            var jobs = connection.Query<SqlJob>(fetchedJobsSql, new { JOB_IDS = jobIds }).ToList();

            var result = new List<KeyValuePair<string, FetchedJobDto>>(jobs.Count);

            foreach (var job in jobs)
            {
                result.Add(new KeyValuePair<string, FetchedJobDto>(job.Id.ToString(), new FetchedJobDto
                {
                    Job = DeserializeJob(job.InvocationData, job.Arguments),
                    State = job.StateName,
                }));
            }

            return new JobList<FetchedJobDto>(result);
        }

        private Dictionary<DateTime, long> GetHourlyTimelineStats(IDbConnection connection, string type)
        {
            var endDate = DateTime.UtcNow;
            var dates = new List<DateTime>();
            for (var i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }

            var keyMaps = dates.ToDictionary(x => $"stats:{type}:{x:yyyy-MM-dd-HH}", x => x);

            return GetTimelineStats(connection, keyMaps);
        }
    }
}
