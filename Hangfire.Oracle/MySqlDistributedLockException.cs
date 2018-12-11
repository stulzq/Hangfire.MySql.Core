using System;

namespace Hangfire.MySql.Core
{
    public class MySqlDistributedLockException : Exception
    {
        public MySqlDistributedLockException(string message) : base(message)
        {
        }
    }
}
