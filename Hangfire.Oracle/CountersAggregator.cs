using System;
using System.Threading;

using Dapper;

using Hangfire.Logging;
using Hangfire.Server;

namespace Hangfire.Oracle.Core
{
    internal class CountersAggregator : IServerComponent
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(CountersAggregator));

        private const int NumberOfRecordsInSinglePass = 1000;
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromMilliseconds(500);

        private readonly OracleStorage _storage;
        private readonly TimeSpan _interval;

        public CountersAggregator(OracleStorage storage, TimeSpan interval)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _interval = interval;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            Logger.DebugFormat("Aggregating records in 'Counter' table...");

            var removedCount = 0;

            do
            {
                _storage.UseConnection(connection =>
                {
                    removedCount = connection.Execute(
                        GetAggregationQuery(),
                        new { now = DateTime.UtcNow, count = NumberOfRecordsInSinglePass });
                });

                if (removedCount >= NumberOfRecordsInSinglePass)
                {
                    cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                    cancellationToken.ThrowIfCancellationRequested();
                }
            } while (removedCount >= NumberOfRecordsInSinglePass);

            cancellationToken.WaitHandle.WaitOne(_interval);
        }

        public override string ToString()
        {
            return GetType().ToString();
        }

        private static string GetAggregationQuery()
        {
            return @"
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
START TRANSACTION;

INSERT INTO AggregatedCounter (`Key`, Value, ExpireAt)
    SELECT `Key`, SUM(Value) as Value, MAX(ExpireAt) AS ExpireAt 
    FROM (
            SELECT `Key`, Value, ExpireAt
            FROM Counter
            LIMIT @count) tmp
	GROUP BY `Key`
        ON DUPLICATE KEY UPDATE 
            Value = Value + VALUES(Value),
            ExpireAt = GREATEST(ExpireAt,VALUES(ExpireAt));

DELETE FROM `Counter`
LIMIT @count;

COMMIT;";
        }
    }
}
