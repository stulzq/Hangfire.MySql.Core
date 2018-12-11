using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

using Hangfire.Annotations;
using Hangfire.Logging;
using Hangfire.Oracle.Core.JobQueue;
using Hangfire.Oracle.Core.Monitoring;
using Hangfire.Server;
using Hangfire.Storage;

using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Core
{
    public class OracleStorage : JobStorage, IDisposable
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(OracleStorage));

        private readonly string _connectionString;
        private readonly IDbConnection _existingConnection;
        private readonly OracleStorageOptions _options;

        public virtual PersistentJobQueueProviderCollection QueueProviders { get; private set; }

        public OracleStorage(string connectionString)
            : this(connectionString, new OracleStorageOptions())
        {
        }

        public OracleStorage(string connectionString, OracleStorageOptions options)
        {
            if (connectionString == null)
            {
                throw new ArgumentNullException(nameof(connectionString));
            }

            if (options == null)
            {
                throw new ArgumentNullException(nameof(options));
            }

            if (IsConnectionString(connectionString))
            {
                if (!connectionString.ToLower().Contains("ignorecommandtransaction"))
                {
                    if (connectionString.Last() != ';')
                    {
                        connectionString += ";IgnoreCommandTransaction=true;";
                    }
                    else
                    {
                        connectionString += "IgnoreCommandTransaction=true;";
                    }
                }
                _connectionString = connectionString;
            }
            else
            {
                throw new ArgumentException($"Could not find connection string with name '{connectionString}' in application config file");
            }
            _options = options;

            if (options.PrepareSchemaIfNecessary)
            {
                using (var connection = CreateAndOpenConnection())
                {
                    OracleObjectsInstaller.Install(connection);
                }
            }

            InitializeQueueProviders();
        }

        private string ApplyAllowUserVariablesProperty(string connectionString)
        {
            if (connectionString.ToLower().Contains("allow user variables"))
            {
                return connectionString;
            }

            return connectionString + ";Allow User Variables=True;";
        }

        internal OracleStorage(IDbConnection existingConnection)
        {
            _existingConnection = existingConnection ?? throw new ArgumentNullException(nameof(existingConnection));
            _options = new OracleStorageOptions();

            InitializeQueueProviders();
        }

        private void InitializeQueueProviders()
        {
            QueueProviders =
                new PersistentJobQueueProviderCollection(
                    new OracleJobQueueProvider(this, _options));
        }

        public override IEnumerable<IServerComponent> GetComponents()
        {
            yield return new ExpirationManager(this, _options.JobExpirationCheckInterval);
            yield return new CountersAggregator(this, _options.CountersAggregateInterval);
        }

        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for SQL Server job storage:");
            logger.InfoFormat("    Queue poll interval: {0}.", _options.QueuePollInterval);
        }

        public override string ToString()
        {
            const string canNotParseMessage = "<Connection string can not be parsed>";

            try
            {
                var parts = _connectionString.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(x => x.Split(new[] { '=' }, StringSplitOptions.RemoveEmptyEntries))
                    .Select(x => new { Key = x[0].Trim(), Value = x.Length > 1 ? x[1].Trim() : "" })
                    .ToDictionary(x => x.Key, x => x.Value, StringComparer.OrdinalIgnoreCase);

                var builder = new StringBuilder();

                foreach (var alias in new[] { "Data Source", "Server", "Address", "Addr", "Network Address" })
                {
                    if (parts.ContainsKey(alias))
                    {
                        builder.Append(parts[alias]);
                        break;
                    }
                }

                if (builder.Length != 0)
                {
                    builder.Append("@");
                }

                foreach (var alias in new[] { "Database", "Initial Catalog" })
                {
                    if (parts.ContainsKey(alias))
                    {
                        builder.Append(parts[alias]);
                        break;
                    }
                }

                return builder.Length != 0
                    ? $"Server: {builder}"
                    : canNotParseMessage;
            }
            catch (Exception ex)
            {
                Logger.ErrorException(ex.Message, ex);
                return canNotParseMessage;
            }
        }

        public override IMonitoringApi GetMonitoringApi()
        {
            return new OracleMonitoringApi(this, _options.DashboardJobListLimit);
        }

        public override IStorageConnection GetConnection()
        {
            return new OracleStorageConnection(this);
        }

        private bool IsConnectionString(string nameOrConnectionString)
        {
            return nameOrConnectionString.Contains(";");
        }

        internal void UseTransaction([InstantHandle] Action<IDbConnection> action)
        {
            UseTransaction(connection =>
            {
                action(connection);
                return true;
            }, null);
        }

        internal T UseTransaction<T>(
           [InstantHandle] Func<IDbConnection, T> func, IsolationLevel? isolationLevel)
        {
            return UseConnection(connection =>
            {
                using (var transaction = connection.BeginTransaction(isolationLevel ?? _options.TransactionIsolationLevel ?? IsolationLevel.ReadUncommitted))
                {
                    T result = func(connection);
                    transaction.Commit();

                    return result;
                }
            });
        }

        internal void UseConnection([InstantHandle] Action<IDbConnection> action)
        {
            UseConnection(connection =>
            {
                action(connection);
                return true;
            });
        }

        internal T UseConnection<T>([InstantHandle] Func<IDbConnection, T> func)
        {
            IDbConnection connection = null;

            try
            {
                connection = CreateAndOpenConnection();
                return func(connection);
            }
            finally
            {
                ReleaseConnection(connection);
            }
        }

        internal IDbConnection CreateAndOpenConnection()
        {
            if (_existingConnection != null)
            {
                return _existingConnection;
            }

            var connection = new OracleConnection(_connectionString);
            connection.Open();

            return connection;
        }

        internal void ReleaseConnection(IDbConnection connection)
        {
            if (connection != null && !ReferenceEquals(connection, _existingConnection))
            {
                connection.Dispose();
            }
        }
        public void Dispose()
        {
        }
    }
}
