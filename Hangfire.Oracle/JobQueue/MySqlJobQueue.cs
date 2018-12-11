using System;
using System.Data;
using System.Linq;
using System.Threading;

using Dapper;

using Hangfire.Logging;
using Hangfire.Storage;

using MySql.Data.MySqlClient;

namespace Hangfire.Oracle.Core.JobQueue
{
    internal class MySqlJobQueue : IPersistentJobQueue
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(MySqlJobQueue));

        private readonly MySqlStorage _storage;
        private readonly MySqlStorageOptions _options;
        public MySqlJobQueue(MySqlStorage storage, MySqlStorageOptions options)
        {
	        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _options = options ?? throw new ArgumentNullException(nameof(options));
        }

        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException(nameof(queues));
            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", nameof(queues));

            FetchedJob fetchedJob = null;
            MySqlConnection connection;

            do
            {
                cancellationToken.ThrowIfCancellationRequested();
                connection = _storage.CreateAndOpenConnection();
                
                try
                {
                    using (new MySqlDistributedLock(_storage, "JobQueue", TimeSpan.FromSeconds(30)))
                    {
                        var token = Guid.NewGuid().ToString();

                        var nUpdated = connection.Execute(
                            "update JobQueue set FetchedAt = UTC_TIMESTAMP(), FetchToken = @fetchToken " +
                            "where (FetchedAt is null or FetchedAt < DATE_ADD(UTC_TIMESTAMP(), INTERVAL @timeout SECOND)) " +
                            "   and Queue in @queues " +
                            "LIMIT 1;",
                            new
                            {
                                queues = queues,
                                timeout = _options.InvisibilityTimeout.Negate().TotalSeconds,
                                fetchToken = token
                            });

                        if(nUpdated != 0)
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
                catch (MySqlException ex)
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

            return new MySqlFetchedJob(_storage, connection, fetchedJob);
        }

        public void Enqueue(IDbConnection connection, string queue, string jobId)
        {
            Logger.TraceFormat("Enqueue JobId={0} Queue={1}", jobId, queue);
            connection.Execute("insert into JobQueue (JobId, Queue) values (@jobId, @queue)", new {jobId, queue});
        }
    }
}