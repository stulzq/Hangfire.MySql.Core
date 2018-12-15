using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;

using Dapper;

using Hangfire.Logging;
using Hangfire.Server;

namespace Hangfire.Oracle.Core
{
#pragma warning disable 618
    internal class ExpirationManager : IServerComponent
#pragma warning restore 618
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(ExpirationManager));

        private static readonly TimeSpan DefaultLockTimeout = TimeSpan.FromSeconds(30);
        private const string DistributedLockKey = "expirationmanager";
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromSeconds(1);
        private const int NumberOfRecordsInSinglePass = 1000;

        private static readonly List<Tuple<string, bool>> TablesToProcess = new List<Tuple<string, bool>>
        {
            // This list must be sorted in dependency order 
            new Tuple<string, bool>("HF_JOB_PARAMETER", true),
            new Tuple<string, bool>("HF_JOB_QUEUE", true),
            new Tuple<string, bool>("HF_JOB_STATE", true),
            new Tuple<string, bool>("HF_AGGREGATED_COUNTER", false),
            new Tuple<string, bool>("HF_LIST", false),
            new Tuple<string, bool>("HF_SET", false),
            new Tuple<string, bool>("HF_HASH", false),
            new Tuple<string, bool>("HF_JOB", false)
        };

        private readonly OracleStorage _storage;
        private readonly TimeSpan _checkInterval;

        public ExpirationManager(OracleStorage storage)
            : this(storage, TimeSpan.FromHours(1))
        {
        }

        public ExpirationManager(OracleStorage storage, TimeSpan checkInterval)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _checkInterval = checkInterval;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            foreach (var tuple in TablesToProcess)
            {
                Logger.DebugFormat("Removing outdated records from table '{0}'...", tuple.Item1);

                var removedCount = 0;

                do
                {
                    _storage.UseConnection(connection =>
                    {
                        try
                        {
                            Logger.DebugFormat("Deleting records from table: {0}", tuple.Item1);

                            using (new OracleDistributedLock(connection, DistributedLockKey, DefaultLockTimeout, cancellationToken).Acquire())
                            {
                                var query = $"DELETE FROM {tuple.Item1} WHERE EXPIRE_AT < :NOW AND ROWNUM <= :COUNT";
                                if (tuple.Item2)
                                {
                                    query = $"DELETE FROM {tuple.Item1} WHERE JOB_ID IN (SELECT ID FROM HF_JOB WHERE EXPIRE_AT < :NOW AND ROWNUM <= :COUNT)";
                                }
                                removedCount = connection.Execute(query, new { NOW = DateTime.UtcNow, COUNT = NumberOfRecordsInSinglePass });
                            }

                            Logger.DebugFormat("removed records count={0}", removedCount);
                        }
                        catch (DbException ex)
                        {
                            Logger.Error(ex.ToString());
                        }
                    });

                    if (removedCount > 0)
                    {
                        Logger.Trace($"Removed {removedCount} outdated record(s) from '{tuple.Item1}' table.");

                        cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                } while (removedCount > 0);
            }

            cancellationToken.WaitHandle.WaitOne(_checkInterval);
        }

        public override string ToString()
        {
            return GetType().ToString();
        }
    }
}
