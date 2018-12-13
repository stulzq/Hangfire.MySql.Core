using System.Data;

using Dapper;

namespace Hangfire.Oracle.Core.Entities
{
    public static class EntityUtils
    {
        public static long GetNextId(this IDbConnection connection)
        {
            return connection.QuerySingle<long>("SELECT HF_SEQUENCE.NEXTVAL FROM dual");
        }

        public static long GetNextJobId(this IDbConnection connection)
        {
            return connection.QuerySingle<long>("SELECT HF_JOB_ID_SEQ.NEXTVAL FROM dual");
        }
    }
}
