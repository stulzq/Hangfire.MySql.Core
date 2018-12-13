using System;
using System.Data;
using System.Threading;

using Dapper;

using Hangfire.Logging;
using Hangfire.Storage;

using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Core.JobQueue
{
    internal class OracleJobQueue : IPersistentJobQueue
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(OracleJobQueue));

        private readonly OracleStorage _storage;
        private readonly OracleStorageOptions _options;
        public OracleJobQueue(OracleStorage storage, OracleStorageOptions options)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _options = options ?? throw new ArgumentNullException(nameof(options));
        }

        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException(nameof(queues));
            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", nameof(queues));

            FetchedJob fetchedJob = null;
            IDbConnection connection;

            do
            {
                cancellationToken.ThrowIfCancellationRequested();
                connection = _storage.CreateAndOpenConnection();

                try
                {
                    using (new OracleDistributedLock(_storage, "JobQueue", TimeSpan.FromSeconds(30)))
                    {
                        var token = Guid.NewGuid().ToString();

                        var nUpdated = connection.Execute(@"
UPDATE HF_JOB_QUEUE
   SET FETCHED_AT = SYS_EXTRACT_UTC (SYSTIMESTAMP), FETCH_TOKEN = :FETCH_TOKEN
 WHERE (FETCHED_AT IS NULL OR FETCHED_AT < SYS_EXTRACT_UTC (SYSTIMESTAMP) + numToDSInterval(:TIMEOUT, 'second' )) AND (QUEUE IN :QUEUES) AND ROWNUM = 1
",
                            new
                            {
                                QUEUES = queues,
                                TIMEOUT = _options.InvisibilityTimeout.Negate().TotalSeconds,
                                FETCH_TOKEN = token
                            });

                        if (nUpdated != 0)
                        {
                            fetchedJob =
                                connection
                                    .QuerySingle<FetchedJob>(
                                        @"
 SELECT ID as Id, JOB_ID as JobId, QUEUE as Queue
   FROM HF_JOB_QUEUE
  WHERE FETCH_TOKEN = :FETCH_TOKEN
",
                                        new
                                        {
                                            FETCH_TOKEN = token
                                        });
                        }
                    }
                }
                catch (OracleException ex)
                {
                    Logger.ErrorException(ex.Message, ex);
                    _storage.ReleaseConnection(connection);
                    throw;
                }

                if (fetchedJob == null)
                {
                    _storage.ReleaseConnection(connection);

                    cancellationToken.WaitHandle.WaitOne(_options.QueuePollInterval);
                    cancellationToken.ThrowIfCancellationRequested();
                }
            } while (fetchedJob == null);

            return new OracleFetchedJob(_storage, connection, fetchedJob);
        }

        public void Enqueue(IDbConnection connection, string queue, string jobId)
        {
            Logger.TraceFormat("Enqueue JobId={0} Queue={1}", jobId, queue);
            connection.Execute("INSERT INTO HF_JOB_QUEUE (ID, JOB_ID, QUEUE) VALUES (HF_SEQUENCE.NEXTVAL, :JOB_ID, :QUEUE)", new { JOB_ID = jobId, QUEUE = queue });
        }
    }
}