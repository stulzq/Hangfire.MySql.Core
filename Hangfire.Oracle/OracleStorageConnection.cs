using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using Dapper;

using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.Oracle.Core.Entities;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.Oracle.Core
{
    public class OracleStorageConnection : JobStorageConnection
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(OracleStorageConnection));

        private readonly OracleStorage _storage;
        public OracleStorageConnection(OracleStorage storage)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new OracleWriteOnlyTransaction(_storage);
        }

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            return new OracleDistributedLock(_storage, resource, timeout).Acquire();
        }

        public override string CreateExpiredJob(Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
        {
            if (job == null)
            {
                throw new ArgumentNullException(nameof(job));
            }

            if (parameters == null)
            {
                throw new ArgumentNullException(nameof(parameters));
            }

            var invocationData = InvocationData.Serialize(job);

            Logger.TraceFormat("CreateExpiredJob={0}", JobHelper.ToJson(invocationData));

            return _storage.UseConnection(connection =>
            {
                var jobId = connection.GetNextId();
                connection.Execute(
                    @" 
 INSERT INTO MISP.HF_JOB (ID, INVOCATION_DATA, ARGUMENTS, CREATED_AT, EXPIRE_AT) 
      VALUES (:ID, :INVOCATION_DATA, :ARGUMENTS, :CREATED_AT, :EXPIRE_AT)
",
                    new
                    {
                        ID = jobId,
                        INVOCATION_DATA = JobHelper.ToJson(invocationData),
                        ARGUMENTS = invocationData.Arguments,
                        CREATED_AT = createdAt,
                        EXPIRE_AT = createdAt.Add(expireIn)
                    });

                if (parameters.Count > 0)
                {
                    var parameterArray = new object[parameters.Count];
                    var parameterIndex = 0;
                    foreach (var parameter in parameters)
                    {
                        parameterArray[parameterIndex++] = new
                        {
                            JOB_ID = jobId,
                            NAME = parameter.Key,
                            VALUE = parameter.Value
                        };
                    }

                    connection.Execute(@"INSERT INTO MISP.HF_JOB_PARAMETER (ID, NAME, VALUE, JOB_ID) VALUES (MISP.HF_SEQUENCE.NEXTVAL, :NAME, :VALUE, :JOB_ID)", parameterArray);
                }

                return jobId.ToString();
            });
        }

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null || queues.Length == 0)
            {
                throw new ArgumentNullException(nameof(queues));
            }

            var providers = queues
                .Select(queue => _storage.QueueProviders.GetProvider(queue))
                .Distinct()
                .ToArray();

            if (providers.Length != 1)
            {
                throw new InvalidOperationException(
                    $"Multiple provider instances registered for queues: {string.Join(", ", queues)}. You should choose only one type of persistent queues per server instance.");
            }

            var persistentQueue = providers[0].GetJobQueue();
            return persistentQueue.Dequeue(queues, cancellationToken);
        }

        public override void SetJobParameter(string id, string name, string value)
        {
            if (id == null)
            {
                throw new ArgumentNullException(nameof(id));
            }

            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    @" 
 MERGE INTO MISP.HF_JOB_PARAMETER JP
      USING (SELECT * FROM MISP.HF_JOB_PARAMETER) SRC
         ON (JP.ID = SRC.ID)
 WHEN MATCHED THEN
      UPDATE SET VALUE = :VALUE
 WHEN NOT MATCHED THEN
      INSERT (ID, JOB_ID, NAME, VALUE)
      VALUES (MISP.HF_SEQUENCE.NEXTVAL, :JOB_ID, :NAME, :VALUE)
",
                    new { JOB_ID = id, NAME = name, VALUE = value });
            });
        }

        public override string GetJobParameter(string id, string name)
        {
            if (id == null)
            {
                throw new ArgumentNullException(nameof(id));
            }

            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            return _storage.UseConnection(connection =>
                connection.QuerySingleOrDefault<string>(
                    "SELECT VALUE as Value " +
                    "  FROM MISP.HF_JOB_PARAMETER " +
                    " WHERE JOB_ID = :ID AND NAME = :NAME",
                    new { ID = id, NAME = name }));
        }

        public override JobData GetJobData(string jobId)
        {
            if (jobId == null)
            {
                throw new ArgumentNullException(nameof(jobId));
            }

            return _storage.UseConnection(connection =>
            {
                var jobData = connection.QuerySingleOrDefault<SqlJob>(
                            "SELECT INVOCATION_DATA AS InvocationData, STATE_NAME AS StateName, ARGUMENTS AS Arguments, CREATED_AT AS CreatedAt " +
                            "  FROM MISP.HF_JOB " +
                            " WHERE ID = :ID",
                            new { ID = jobId });

                if (jobData == null)
                {
                    return null;
                }

                var invocationData = JobHelper.FromJson<InvocationData>(jobData.InvocationData);
                invocationData.Arguments = jobData.Arguments;

                Job job = null;
                JobLoadException loadException = null;

                try
                {
                    job = invocationData.Deserialize();
                }
                catch (JobLoadException ex)
                {
                    loadException = ex;
                }

                return new JobData
                {
                    Job = job,
                    State = jobData.StateName,
                    CreatedAt = jobData.CreatedAt,
                    LoadException = loadException
                };
            });
        }

        public override StateData GetStateData(string jobId)
        {
            if (jobId == null)
            {
                throw new ArgumentNullException(nameof(jobId));
            }

            return _storage.UseConnection(connection =>
            {
                var sqlState = connection.QuerySingleOrDefault<SqlState>(
                        " SELECT S.NAME AS Name, S.REASON AS Reason, S.DATA AS Data " +
                        "   FROM MISP.HF_JOB_STATE S" +
                        "  INNER JOIN MISP.HF_JOB J" +
                        "    ON J.STATE_ID = S.ID " +
                        " WHERE J.ID = :JOB_ID",
                        new { JOB_ID = jobId });

                if (sqlState == null)
                {
                    return null;
                }

                var data = new Dictionary<string, string>(
                    JobHelper.FromJson<Dictionary<string, string>>(sqlState.Data),
                    StringComparer.OrdinalIgnoreCase);

                return new StateData
                {
                    Name = sqlState.Name,
                    Reason = sqlState.Reason,
                    Data = data
                };
            });
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            if (serverId == null)
            {
                throw new ArgumentNullException(nameof(serverId));
            }

            if (context == null)
            {
                throw new ArgumentNullException(nameof(context));
            }

            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    @"
 MERGE INTO MISP.HF_SERVER S
      USING (SELECT 1 FROM DUAL) SRC
         ON (S.ID = :ID)
 WHEN MATCHED THEN
      UPDATE SET LAST_HEART_BEAT = :LAST_HEART_BEAT
 WHEN NOT MATCHED THEN
      INSERT (ID, DATA, LAST_HEART_BEAT)
      VALUES (:ID, :DATA, :LAST_HEART_BEAT)
",
                    new
                    {
                        ID = serverId,
                        DATA = JobHelper.ToJson(new ServerData
                        {
                            WorkerCount = context.WorkerCount,
                            Queues = context.Queues,
                            StartedAt = DateTime.UtcNow,
                        }),
                        LAST_HEART_BEAT = DateTime.UtcNow
                    });
            });
        }

        public override void RemoveServer(string serverId)
        {
            if (serverId == null)
            {
                throw new ArgumentNullException(nameof(serverId));
            }

            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    "DELETE FROM MISP.HF_SERVER where ID = :ID",
                    new { ID = serverId });
            });
        }

        public override void Heartbeat(string serverId)
        {
            if (serverId == null)
            {
                throw new ArgumentNullException(nameof(serverId));
            }

            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    " UPDATE MISP.HF_SERVER" +
                    "    SET LAST_HEART_BEAT = :NOW" +
                    "  WHERE ID = :ID",
                    new { NOW = DateTime.UtcNow, ID = serverId });
            });
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
            {
                throw new ArgumentException("The `timeOut` value must be positive.", nameof(timeOut));
            }

            return
                _storage.UseConnection(connection =>
                    connection.Execute(
                        " DELETE FROM MISP.HF_SERVER" +
                        "  WHERE LAST_HEART_BEAT < :TIME_OUT_AT",
                        new { TIME_OUT_AT = DateTime.UtcNow.Add(timeOut.Negate()) }));
        }

        public override long GetSetCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return
                _storage.UseConnection(connection =>
                    connection.QueryFirst<int>(
                        "SELECT COUNT(KEY) " +
                        "  FROM MISP.HF_SET" +
                        " WHERE KEY = :KEY",
                        new { KEY = key }));
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return _storage.UseConnection(connection =>
                connection.Query<string>(@"
SELECT VALUE as Value
  FROM (SELECT VALUE, RANK () OVER (ORDER BY ID) AS RANK
          FROM MISP.HF_SET
         WHERE KEY = :KEY)
 WHERE RANK BETWEEN :S AND :E
",
                        new { KEY = key, S = startingFrom + 1, E = endingAt + 1 }).ToList());
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return
                _storage.UseConnection(connection =>
                {
                    var result = connection.Query<string>(
                        "SELECT VALUE AS Value" +
                        "  FROM MISP.HF_SET" +
                        " WHERE KEY = :KEY",
                        new { KEY = key });

                    return new HashSet<string>(result);
                });
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (toScore < fromScore)
            {
                throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");
            }

            return
                _storage.UseConnection(connection =>
                    connection.QuerySingleOrDefault<string>(
                        @"
SELECT *
  FROM (  SELECT VALUE AS VALUE
            FROM MISP.HF_SET
           WHERE KEY = :KEY AND SCORE BETWEEN :F AND :T
        ORDER BY SCORE)
 WHERE ROWNUM = 1
",
                        new { KEY = key, F = fromScore, T = toScore }));
        }

        public override long GetCounter(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            const string query = @"
 SELECT SUM(S.Value)
   FROM (SELECT SUM(VALUE) AS Value
   FROM MISP.HF_COUNTER
  WHERE KEY = :KEY
 UNION ALL
 SELECT VALUE as Value
  FROM MISP.HF_AGGREGATED_COUNTER
 WHERE KEY = :KEY) AS S";

            return
                _storage
                    .UseConnection(connection =>
                        connection.QuerySingle<long?>(query, new { KEY = key }) ?? 0);
        }

        public override long GetHashCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return
                _storage
                    .UseConnection(connection =>
                        connection.QuerySingle<long>(
                            "SELECT COUNT(ID) FROM MISP.HF_HASH WHERE KEY = :KEY",
                            new { KEY = key }));
        }

        public override TimeSpan GetHashTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _storage.UseConnection(connection =>
            {
                var result =
                    connection.QuerySingle<DateTime?>(
                        "SELECT MIN(EXPIRE_AT) FROM MISP.HF_HASH WHERE KEY = :KEY",
                        new { KEY = key });

                if (!result.HasValue)
                {
                    return TimeSpan.FromSeconds(-1);
                }

                return result.Value - DateTime.UtcNow;
            });
        }

        public override long GetListCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return
                _storage
                    .UseConnection(connection =>
                        connection.QuerySingle<long>(
                            "SELECT COUNT(ID) FROM MISP.HF_LIST WHERE KEY = :KEY",
                            new { KEY = key }));
        }

        public override TimeSpan GetListTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _storage.UseConnection(connection =>
            {
                var result = connection.QuerySingle<DateTime?>(
                        "SELECT MIN(EXPIRE_AT) FROM MISP.HF_LIST WHERE KEY = :KEY",
                        new { KEY = key });

                if (!result.HasValue)
                {
                    return TimeSpan.FromSeconds(-1);
                }

                return result.Value - DateTime.UtcNow;
            });
        }

        public override string GetValueFromHash(string key, string name)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (name == null) throw new ArgumentNullException(nameof(name));

            return
                _storage
                    .UseConnection(connection =>
                        connection.QuerySingleOrDefault<string>(
                            "SELECT VALUE AS Value FROM MISP.HF_HASH WHERE KEY = :KEY and FIELD = :FIELD",
                            new { KEY = key, FIELD = name }));
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            const string query = @"
SELECT VALUE as Value
  FROM (SELECT VALUE, RANK () OVER (ORDER BY ID DESC) AS RANK
          FROM MISP.HF_LIST
         WHERE KEY = :KEY)
 WHERE RANK BETWEEN :S AND :E
";
            return
                _storage
                    .UseConnection(connection =>
                        connection.Query<string>(query,
                            new { KEY = key, S = startingFrom + 1, E = endingAt + 1 })
                            .ToList());
        }

        public override List<string> GetAllItemsFromList(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            const string query = @"
 SELECT VALUE AS Value
   FROM MISP.HF_LIST
  WHERE KEY = :KEY
 ORDER BY ID DESC";

            return _storage.UseConnection(connection => connection.Query<string>(query, new { KEY = key }).ToList());
        }

        public override TimeSpan GetSetTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _storage.UseConnection(connection =>
            {
                var result =
                    connection
                        .QuerySingle<DateTime?>(
                            "SELECT MIN(EXPIRE_AT) FROM MISP.HF_SET WHERE KEY = :KEY",
                            new { KEY = key });

                if (!result.HasValue)
                {
                    return TimeSpan.FromSeconds(-1);
                }

                return result.Value - DateTime.UtcNow;
            });
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (keyValuePairs == null)
            {
                throw new ArgumentNullException(nameof(keyValuePairs));
            }

            _storage.UseTransaction(connection =>
            {
                foreach (var keyValuePair in keyValuePairs)
                {
                    connection.Execute(
                        @"
 MERGE INTO MISP.HF_HASH H
      USING (SELECT * FROM MISP.HF_HASH) SRC
         ON (H.KEY = SRC.KEY AND H.FIELD = SRC.FIELD)
 WHEN MATCHED THEN
     UPDATE SET VALUE = :VALUE
 WHEN NOT MATCHED THEN
     INSERT (ID, KEY, FIELD, VALUE)
     VALUES (MISP.HF_SEQUENCE.NEXTVAL, :KEY, :FIELD, :VALUE)
",
                        new { KEY = key, FIELD = keyValuePair.Key, VALUE = keyValuePair.Value });
                }
            });
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return _storage.UseConnection(connection =>
            {
                var result = connection.Query<SqlHash>(
                    "SELECT FIELD AS Field, VALUE AS Value FROM MISP.HF_HASH WHERE KEY = :KEY",
                    new { KEY = key })
                    .ToDictionary(x => x.Field, x => x.Value);

                return result.Count != 0 ? result : null;
            });
        }
    }
}