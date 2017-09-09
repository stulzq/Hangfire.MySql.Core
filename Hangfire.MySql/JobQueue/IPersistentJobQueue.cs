using System.Data;
using System.Threading;
using Hangfire.Storage;

namespace Hangfire.MySql.Core.JobQueue
{
    public interface IPersistentJobQueue
    {
        IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken);
        void Enqueue(IDbConnection connection, string queue, string jobId);
    }
}