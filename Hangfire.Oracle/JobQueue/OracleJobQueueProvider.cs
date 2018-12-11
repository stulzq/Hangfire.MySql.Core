using System;

namespace Hangfire.Oracle.Core.JobQueue
{
    internal class OracleJobQueueProvider : IPersistentJobQueueProvider
    {
        private readonly IPersistentJobQueue _jobQueue;
        private readonly IPersistentJobQueueMonitoringApi _monitoringApi;

        public OracleJobQueueProvider(OracleStorage storage, OracleStorageOptions options)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));
            if (options == null) throw new ArgumentNullException(nameof(options));

            _jobQueue = new OracleJobQueue(storage, options);
            _monitoringApi = new OracleJobQueueMonitoringApi(storage);
        }

        public IPersistentJobQueue GetJobQueue()
        {
            return _jobQueue;
        }

        public IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi()
        {
            return _monitoringApi;
        }
    }
}