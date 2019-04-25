using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;

using Dapper;
using Dapper.Oracle;

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

            var invocationData = InvocationData.SerializeJob(job);
            invocationData.Arguments = null;
            var arguments = InvocationData.SerializeJob(job);

            Logger.TraceFormat("CreateExpiredJob={0}", SerializationHelper.Serialize(invocationData, SerializationOption.User));

            return _storage.UseConnection(connection =>
            {
                var jobId = connection.GetNextJobId();

                var oracleDynamicParameters = new OracleDynamicParameters();
                oracleDynamicParameters.AddDynamicParams(new
                {
                    ID = jobId,
                    CREATED_AT = createdAt,
                    EXPIRE_AT = createdAt.Add(expireIn)
                });
                oracleDynamicParameters.Add("INVOCATION_DATA", SerializationHelper.Serialize(invocationData, SerializationOption.User), OracleMappingType.NClob, ParameterDirection.Input);
                oracleDynamicParameters.Add("ARGUMENTS", arguments.Arguments, OracleMappingType.NClob, ParameterDirection.Input);

                connection.Execute(
                    @" 
 INSERT INTO HF_JOB (ID, INVOCATION_DATA, ARGUMENTS, CREATED_AT, EXPIRE_AT) 
      VALUES (:ID, :INVOCATION_DATA, :ARGUMENTS, :CREATED_AT, :EXPIRE_AT)
",
                    oracleDynamicParameters);

                if (parameters.Count > 0)
                {
                    var parameterArray = new object[parameters.Count];
                    var parameterIndex = 0;
                    foreach (var parameter in parameters)
                    {
                        var dynamicParameters = new OracleDynamicParameters();
                        dynamicParameters.AddDynamicParams(new
                        {
                            JOB_ID = jobId,
                            NAME = parameter.Key
                        });
                        dynamicParameters.Add("VALUE", parameter.Value, OracleMappingType.NClob, ParameterDirection.Input);

                        parameterArray[parameterIndex++] = dynamicParameters;
                    }

                    connection.Execute(@"INSERT INTO HF_JOB_PARAMETER (ID, NAME, VALUE, JOB_ID) VALUES (HF_SEQUENCE.NEXTVAL, :NAME, :VALUE, :JOB_ID)", parameterArray);
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
                var oracleDynamicParameters = new OracleDynamicParameters();
                oracleDynamicParameters.AddDynamicParams(new { JOB_ID = id, NAME = name });
                oracleDynamicParameters.Add("VALUE", value, OracleMappingType.NClob, ParameterDirection.Input);
                connection.Execute(
                    @" 
 MERGE INTO HF_JOB_PARAMETER JP
      USING (SELECT 1 FROM DUAL) SRC
         ON (JP.NAME = :NAME AND JP.JOB_ID = :JOB_ID)
 WHEN MATCHED THEN
      UPDATE SET VALUE = :VALUE
 WHEN NOT MATCHED THEN
      INSERT (ID, JOB_ID, NAME, VALUE)
      VALUES (HF_SEQUENCE.NEXTVAL, :JOB_ID, :NAME, :VALUE)
",
                    oracleDynamicParameters);
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
                    "  FROM HF_JOB_PARAMETER " +
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
                            "  FROM HF_JOB " +
                            " WHERE ID = :ID",
                            new { ID = jobId });

                if (jobData == null)
                {
                    return null;
                }

                var invocationData = SerializationHelper.Deserialize<InvocationData>(jobData.InvocationData, SerializationOption.User);
                invocationData.Arguments = jobData.Arguments;

                Job job = null;
                JobLoadException loadException = null;

                try
                {
                    job = invocationData.DeserializeJob();
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
                        "   FROM HF_JOB_STATE S" +
                        "  INNER JOIN HF_JOB J" +
                        "    ON J.STATE_ID = S.ID " +
                        " WHERE J.ID = :JOB_ID",
                        new { JOB_ID = jobId });

                if (sqlState == null)
                {
                    return null;
                }

                var data = new Dictionary<string, string>(
                    SerializationHelper.Deserialize<Dictionary<string, string>>(sqlState.Data, SerializationOption.User),
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
 MERGE INTO HF_SERVER S
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
                        DATA = SerializationHelper.Serialize(new ServerData
                        {
                            WorkerCount = context.WorkerCount,
                            Queues = context.Queues,
                            StartedAt = DateTime.UtcNow,
                        }, SerializationOption.User),
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
                    "DELETE FROM HF_SERVER where ID = :ID",
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
                    " UPDATE HF_SERVER" +
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
                        " DELETE FROM HF_SERVER" +
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
                        "  FROM HF_SET" +
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
          FROM HF_SET
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
                        "  FROM HF_SET" +
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
  FROM (  SELECT VALUE AS Value
            FROM HF_SET
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
   FROM HF_COUNTER
  WHERE KEY = :KEY
 UNION ALL
 SELECT VALUE as Value
  FROM HF_AGGREGATED_COUNTER
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
                            "SELECT COUNT(ID) FROM HF_HASH WHERE KEY = :KEY",
                            new { KEY = key }));
        }

        public override TimeSpan GetHashTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _storage.UseConnection(connection =>
            {
                var result =
                    connection.QuerySingle<DateTime?>(
                        "SELECT MIN(EXPIRE_AT) FROM HF_HASH WHERE KEY = :KEY",
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
                            "SELECT COUNT(ID) FROM HF_LIST WHERE KEY = :KEY",
                            new { KEY = key }));
        }

        public override TimeSpan GetListTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _storage.UseConnection(connection =>
            {
                var result = connection.QuerySingle<DateTime?>(
                        "SELECT MIN(EXPIRE_AT) FROM HF_LIST WHERE KEY = :KEY",
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
                            "SELECT VALUE AS Value FROM HF_HASH WHERE KEY = :KEY and FIELD = :FIELD",
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
          FROM HF_LIST
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
   FROM HF_LIST
  WHERE KEY = :KEY
 ORDER BY ID DESC";

            return _storage.UseConnection(connection => connection.Query<string>(query, new { KEY = key }).ToList());
        }

        public override TimeSpan GetSetTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _storage.UseConnection(connection =>
            {
                var result = connection.QuerySingle<DateTime?>("SELECT MIN(EXPIRE_AT) FROM HF_SET WHERE KEY = :KEY", new { KEY = key });

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
                    var oracleDynamicParameters = new OracleDynamicParameters();
                    oracleDynamicParameters.AddDynamicParams(new { KEY = key, FIELD = keyValuePair.Key });
                    oracleDynamicParameters.Add("VALUE", keyValuePair.Value, OracleMappingType.NClob, ParameterDirection.Input);

                    connection.Execute(
                        @"
 MERGE INTO HF_HASH H
      USING (SELECT 1 FROM DUAL) SRC
         ON (H.KEY = :KEY AND H.FIELD = :FIELD)
 WHEN MATCHED THEN
     UPDATE SET VALUE = :VALUE
 WHEN NOT MATCHED THEN
     INSERT (ID, KEY, FIELD, VALUE)
     VALUES (HF_SEQUENCE.NEXTVAL, :KEY, :FIELD, :VALUE)
",
                        oracleDynamicParameters);
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
                    "SELECT FIELD AS Field, VALUE AS Value FROM HF_HASH WHERE KEY = :KEY",
                    new { KEY = key })
                    .ToDictionary(x => x.Field, x => x.Value);

                return result.Count != 0 ? result : null;
            });
        }
    }
}