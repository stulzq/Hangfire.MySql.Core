using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Hangfire.Annotations;
using Hangfire.Logging;
using Hangfire.MySql.JobQueue;
using Hangfire.MySql.Monitoring;
using Hangfire.Server;
using Hangfire.Storage;
using MySql.Data.MySqlClient;

namespace Hangfire.MySql
{
    public class MySqlStorage : JobStorage, IDisposable
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(MySqlStorage));

        private readonly string _connectionString;
        private readonly MySqlConnection _existingConnection;
        private readonly MySqlStorageOptions _options;

        public virtual PersistentJobQueueProviderCollection QueueProviders { get; private set; }

        public MySqlStorage(string connectionString)
            : this(connectionString, new MySqlStorageOptions())
        {
        }

        public MySqlStorage(string connectionString, MySqlStorageOptions options)
        {
            if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
            if (options == null) throw new ArgumentNullException(nameof(options));

            if (IsConnectionString(connectionString))
            {
                _connectionString = ApplyAllowUserVariablesProperty(connectionString);
            }
            else
            {
                throw new ArgumentException(
                    $"Could not find connection string with name '{connectionString}' in application config file");
            }

            _options = options;

            if (options.PrepareSchemaIfNecessary)
            {
                using (var connection = CreateAndOpenConnection())
                {
                    MySqlObjectsInstaller.Install(connection);
                }
            }

            InitializeQueueProviders();
        }

        private string ApplyAllowUserVariablesProperty(string connectionString)
        {
            return connectionString.ToLower().Contains("allow user variables")
                ? connectionString
                : $"{connectionString};Allow User Variables=True;";
        }

        internal MySqlStorage(MySqlConnection existingConnection)
        {
            _existingConnection = existingConnection ?? throw new ArgumentNullException(nameof(existingConnection));
            _options = new MySqlStorageOptions();

            InitializeQueueProviders();
        }

        private void InitializeQueueProviders()
        {
            QueueProviders =
                new PersistentJobQueueProviderCollection(
                    new MySqlJobQueueProvider(this, _options));
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
                var parts = _connectionString.Split(new[] {';'}, StringSplitOptions.RemoveEmptyEntries)
                    .Select(x => x.Split(new[] {'='}, StringSplitOptions.RemoveEmptyEntries))
                    .Select(x => new {Key = x[0].Trim(), Value = x.Length > 1 ? x[1].Trim() : ""})
                    .ToDictionary(x => x.Key, x => x.Value, StringComparer.OrdinalIgnoreCase);

                var builder = new StringBuilder();

                foreach (var alias in new[] {"Data Source", "Server", "Address", "Addr", "Network Address"})
                {
                    if (parts.ContainsKey(alias))
                    {
                        builder.Append(parts[alias]);
                        break;
                    }
                }

                if (builder.Length != 0) builder.Append("@");

                foreach (var alias in new[] {"Database", "Initial Catalog"})
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
            return new MySqlMonitoringApi(this, _options.DashboardJobListLimit);
        }

        public override IStorageConnection GetConnection()
        {
            return new MySqlStorageConnection(this);
        }

        private bool IsConnectionString(string nameOrConnectionString)
        {
            return nameOrConnectionString.Contains(";");
        }

        internal void UseTransaction([InstantHandle] Action<MySqlConnection> action)
        {
            UseTransaction(connection =>
            {
                action(connection);
                return true;
            }, null);
        }

        internal T UseTransaction<T>(
            [InstantHandle] Func<MySqlConnection, T> func, IsolationLevel? isolationLevel)
        {
            return UseConnection(connection =>
            {
                T result = func(connection);

                return result;
            });
        }

        internal void UseConnection([InstantHandle] Action<MySqlConnection> action)
        {
            UseConnection(connection =>
            {
                action(connection);
                return true;
            });
        }

        internal T UseConnection<T>([InstantHandle] Func<MySqlConnection, T> func)
        {
            MySqlConnection connection = null;

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

        internal MySqlConnection CreateAndOpenConnection()
        {
            if (_existingConnection != null)
            {
                return _existingConnection;
            }

            var connection = new MySqlConnection(_connectionString);
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