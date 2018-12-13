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
                    removedCount = connection.Execute(GetMergeQuery(), new { COUNT = NumberOfRecordsInSinglePass });
                    //removedCount = connection.Execute(GetDeleteQuery(), new { COUNT = NumberOfRecordsInSinglePass });
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

        private static string GetMergeQuery()
        {
            return @"
BEGIN
    MERGE INTO HF_AGGREGATED_COUNTER AC
         USING (  SELECT KEY, SUM (VALUE) AS VALUE, MAX (EXPIRE_AT) AS EXPIRE_AT
                    FROM (SELECT KEY, VALUE, EXPIRE_AT
                            FROM HF_COUNTER
                           WHERE ROWNUM <= :COUNT) TMP
                GROUP BY KEY) C
            ON (AC.KEY = C.KEY)
    WHEN MATCHED
    THEN
       UPDATE SET VALUE = VALUE + C.VALUE, EXPIRE_AT = GREATEST (EXPIRE_AT, C.EXPIRE_AT)
    WHEN NOT MATCHED
    THEN
       INSERT     (ID
                  ,KEY
                  ,VALUE
                  ,EXPIRE_AT)
           VALUES (HF_SEQUENCE.NEXTVAL
                  ,C.KEY
                  ,C.VALUE
                  ,C.EXPIRE_AT);

   DELETE FROM HF_COUNTER
    WHERE ROWNUM <= :COUNT;
END;
";
        }

        private static string GetDeleteQuery()
        {
            return @"
 DELETE FROM HF_COUNTER
  WHERE ROWNUM <= :COUNT
";
        }
    }
}
