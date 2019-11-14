using System;

namespace Hangfire.MySql.Core
{
    public static class MySqlStorageExtensions
    {
        public static IGlobalConfiguration<MySqlStorage> UseMySqlStorage(this IGlobalConfiguration configuration,string connectionString)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));

            var storage = new MySqlStorage(connectionString);
            return configuration.UseStorage(storage);
        }

        public static IGlobalConfiguration<MySqlStorage> UseMySqlStorage(this IGlobalConfiguration configuration,string connectionString, MySqlStorageOptions options)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
            if (options == null) throw new ArgumentNullException(nameof(options));

            var storage = new MySqlStorage(connectionString, options);
            return configuration.UseStorage(storage);
        }
    }
}
