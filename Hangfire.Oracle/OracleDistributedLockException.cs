using System;

namespace Hangfire.Oracle.Core
{
    public class OracleDistributedLockException : Exception
    {
        public OracleDistributedLockException(string message) : base(message)
        {
        }
    }
}
