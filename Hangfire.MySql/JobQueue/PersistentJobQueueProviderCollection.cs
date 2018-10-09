﻿using System;
using System.Collections;
using System.Collections.Generic;
using Hangfire.Logging;

namespace Hangfire.MySql.Core.JobQueue
{
    public class PersistentJobQueueProviderCollection : IEnumerable<IPersistentJobQueueProvider>
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(PersistentJobQueueProviderCollection));

        private readonly List<IPersistentJobQueueProvider> _providers
            = new List<IPersistentJobQueueProvider>();
        private readonly Dictionary<string, IPersistentJobQueueProvider> _providersByQueue
            = new Dictionary<string, IPersistentJobQueueProvider>(StringComparer.OrdinalIgnoreCase);

        private readonly IPersistentJobQueueProvider _defaultProvider;

        public PersistentJobQueueProviderCollection(IPersistentJobQueueProvider defaultProvider)
        {
            _defaultProvider = defaultProvider ?? throw new ArgumentNullException(nameof(defaultProvider));

            _providers.Add(_defaultProvider);
        }

        public void Add(IPersistentJobQueueProvider provider, IEnumerable<string> queues)
        {
            if (provider == null) throw new ArgumentNullException(nameof(provider));
            if (queues == null) throw new ArgumentNullException(nameof(queues));

            Logger.TraceFormat("Add providers");

            _providers.Add(provider);

            foreach (var queue in queues)
            {
                _providersByQueue.Add(queue, provider);
            }
        }

        public IPersistentJobQueueProvider GetProvider(string queue)
        {
            return _providersByQueue.ContainsKey(queue)
                ? _providersByQueue[queue]
                : _defaultProvider;
        }

        public IEnumerator<IPersistentJobQueueProvider> GetEnumerator()
        {
            return _providers.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
