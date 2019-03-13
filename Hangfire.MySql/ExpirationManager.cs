using System;
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.Server;
using MySql.Data.MySqlClient;

namespace Hangfire.MySql.Core
{
    internal class ExpirationManager : IServerComponent
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(ExpirationManager));

        private static readonly TimeSpan DefaultLockTimeout = TimeSpan.FromSeconds(30);
        private const string DistributedLockKey = "expirationmanager";
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromSeconds(1);
        private const int NumberOfRecordsInSinglePass = 1000;

        private string[] ProcessedTables;

        private readonly MySqlStorage _storage;
        private readonly MySqlStorageOptions _options;
        private readonly TimeSpan _checkInterval;

        public ExpirationManager(MySqlStorage storage,MySqlStorageOptions options)
            : this(storage, options,TimeSpan.FromHours(1))
        {
        }

        public ExpirationManager(MySqlStorage storage,MySqlStorageOptions options, TimeSpan checkInterval)
        {
            if (storage == null) throw new ArgumentNullException("storage");

            _storage = storage;
            _options = options;
            _checkInterval = checkInterval;
            ProcessedTables = new string[]{ "AggregatedCounter", "Job", "List", "Set",  "Hash" };
        }

        public void Execute(CancellationToken cancellationToken)
        {
            foreach (var table in ProcessedTables)
            {
                Logger.DebugFormat("Removing outdated records from table '{0}'...", _options.TablePrefix+"_"+table);

                int removedCount = 0;

                do
                {
                    _storage.UseConnection(connection =>
                    {
                        try
                        {
                            Logger.DebugFormat("delete from `{0}` where ExpireAt < @now limit @count;", _options.TablePrefix + "_" + table);

                            using (
                                new MySqlDistributedLock(
                                    connection, 
                                    _options,
                                    DistributedLockKey, 
                                    DefaultLockTimeout,
                                    cancellationToken).Acquire())
                            {
                                removedCount = connection.Execute(
                                    String.Format(
                                        "delete from `{0}` where ExpireAt < @now limit @count;", _options.TablePrefix + "_" + table),
                                    new {now = DateTime.UtcNow, count = NumberOfRecordsInSinglePass});
                            }

                            Logger.DebugFormat("removed records count={0}",removedCount);
                        }
                        catch (MySqlException ex)
                        {
                            Logger.Error(ex.ToString());
                        }
                    });

                    if (removedCount > 0)
                    {
                        Logger.Trace(String.Format("Removed {0} outdated record(s) from '{1}' table.", removedCount,
                            _options.TablePrefix + "_" + table));

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
