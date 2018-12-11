using System;
using System.Data;
using System.Linq;
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

                        var nUpdated = connection.Execute(
                            "UPDATE JobQueue " +
                            "   SET FetchedAt = SYS_EXTRACT_UTC(SYSTIMESTAMP), FetchToken = :FETCH_TOKEN " +
                            " WHERE (FetchedAt IS NULL OR FetchedAt < SYS_EXTRACT_UTC(SYSTIMESTAMP) + INTERVAL :TIMEOUT SECOND) " +
                            "   AND Queue IN :QUEUES " +
                            "   AND ROWNUM = 1;",
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
                                    .Query<FetchedJob>(
                                        "select Id, JobId, Queue " +
                                        "from JobQueue " +
                                        "where FetchToken = @fetchToken;",
                                        new
                                        {
                                            fetchToken = token
                                        })
                                    .SingleOrDefault();
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
            connection.Execute("INSERT INTO JobQueue (JobId, Queue) values (:JOB_ID, :QUEUE)", new { JOB_ID = jobId, QUEUE = queue });
        }
    }
}