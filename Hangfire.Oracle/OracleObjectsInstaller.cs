using System;
using System.Data;
using System.IO;
using System.Reflection;

using Dapper;

using Hangfire.Logging;

namespace Hangfire.Oracle.Core
{
    public static class OracleObjectsInstaller
    {
        private static readonly ILog Log = LogProvider.GetLogger(typeof(OracleStorage));
        public static void Install(IDbConnection connection)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));

            if (TablesExists(connection))
            {
                Log.Info("DB tables already exist. Exit install");
                return;
            }

            Log.Info("Start installing Hangfire SQL objects...");

            var script = GetStringResource("Hangfire.Oracle.Core.Install.sql");

            connection.Execute(script);

            Log.Info("Hangfire SQL objects installed.");
        }

        private static bool TablesExists(IDbConnection connection)
        {
            return connection.ExecuteScalar<string>(@"
   SELECT TABLE_NAME
     FROM all_tables
    WHERE OWNER = 'MISP' AND TABLE_NAME LIKE 'HF_%'
 ORDER BY OWNER, TABLE_NAME
") != null;
        }

        private static string GetStringResource(string resourceName)
        {
#if NET45
            var assembly = typeof(OracleObjectsInstaller).Assembly;
#else
            var assembly = typeof(OracleObjectsInstaller).GetTypeInfo().Assembly;
#endif

            using (var stream = assembly.GetManifestResourceStream(resourceName))
            {
                if (stream == null)
                {
                    throw new InvalidOperationException($"Requested resource `{resourceName}` was not found in the assembly `{assembly}`.");
                }

                using (var reader = new StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
        }
    }
}