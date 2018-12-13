using System;
using System.Threading;

using Dapper;

using Hangfire.Logging;
using Hangfire.Server;

using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Core
{
    internal class ExpirationManager : IServerComponent
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(ExpirationManager));

        private static readonly TimeSpan DefaultLockTimeout = TimeSpan.FromSeconds(30);
        private const string DistributedLockKey = "expirationmanager";
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromSeconds(1);
        private const int NumberOfRecordsInSinglePass = 1000;

        private static readonly string[] ProcessedTables =
        {
            "HF_AGGREGATED_COUNTER",
            "HF_JOB",
            "HF_LIST",
            "HF_SET",
            "HF_HASH",
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
            foreach (var table in ProcessedTables)
            {
                Logger.DebugFormat("Removing outdated records from table '{0}'...", table);

                var removedCount = 0;

                do
                {
                    _storage.UseConnection(connection =>
                    {
                        try
                        {
                            Logger.DebugFormat("DELETE FROM `{0}` WHERE EXPIRE_AT < :NOW AND ROWNUM <= :COUNT", table);

                            using (
                                new OracleDistributedLock(
                                    connection,
                                    DistributedLockKey,
                                    DefaultLockTimeout,
                                    cancellationToken).Acquire())
                            {
                                removedCount = connection.Execute(
                                    $"DELETE FROM {table} WHERE EXPIRE_AT < :NOW AND ROWNUM <= :COUNT",
                                    new { NOW = DateTime.UtcNow, COUNT = NumberOfRecordsInSinglePass });
                            }

                            Logger.DebugFormat("removed records count={0}", removedCount);
                        }
                        catch (OracleException ex)
                        {
                            Logger.Error(ex.ToString());
                        }
                    });

                    if (removedCount > 0)
                    {
                        Logger.Trace($"Removed {removedCount} outdated record(s) from '{table}' table.");

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
