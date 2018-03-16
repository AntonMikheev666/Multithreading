using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class RoundRobinClusterClient : ClusterClientBase
    {
        public RoundRobinClusterClient(string[] replicaAddresses)
            : base(replicaAddresses)
        {
        }

        public async override Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            timeout = new TimeSpan((long)(timeout.Ticks * 1.0 / ReplicaAddresses.Length));
            
            foreach (var uri in ReplicaAddresses)
            {
                var url = uri;
                var webRequest = CreateRequest(url + "?query=" + query);

                Log.InfoFormat("Processing {0}", webRequest.RequestUri);

                var resultTask = ProcessRequestAsync(webRequest);
                await Task.WhenAny(resultTask, Task.Delay(timeout));
                if (!resultTask.IsCompleted)
                    continue;

                if (!resultTask.IsFaulted)
                    return resultTask.Result;
            }

            throw new TimeoutException();
        }

        protected override ILog Log
        {
            get { return LogManager.GetLogger(typeof(RandomClusterClient)); }
        }
    }
}