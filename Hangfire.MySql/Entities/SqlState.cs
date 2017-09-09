using System;

namespace Hangfire.MySql.Core.Entities
{
    internal class SqlState
    {
        public int JobId { get; set; }
        public string Name { get; set; }
        public string Reason { get; set; }
        public DateTime CreatedAt { get; set; }
        public string Data { get; set; }
    }
}
