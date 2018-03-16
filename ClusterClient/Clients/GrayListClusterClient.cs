using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class GrayListClusterClient : ClusterClientBase
    {
        private ConcurrentQueue<string> grayURIs;
        private TimeSpan grayTTL;

        public GrayListClusterClient(string[] replicaAddresses, TimeSpan grayTTL)
            : base(replicaAddresses)
        {
            grayURIs = new ConcurrentQueue<string>();
            this.grayTTL = grayTTL;
        }

        public async override Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            timeout = new TimeSpan((long)(timeout.Ticks * 1.0 / ReplicaAddresses.Length));
            
            foreach (var uri in ReplicaAddresses)
            {
                if (grayURIs.Contains(uri))
                    continue;

                var url = uri;
                var webRequest = CreateRequest(url + "?query=" + query);

                Log.InfoFormat("Processing {0}", webRequest.RequestUri);

                var resultTask = ProcessRequestAsync(webRequest);
                await Task.WhenAny(resultTask, Task.Delay(timeout));
                if (!resultTask.IsCompleted)
                    await AddToGrays(url);

                if(!resultTask.IsFaulted)
                    return resultTask.Result;
            }

            throw new TimeoutException();
        }

        private async Task AddToGrays(string uri)
        {
            grayURIs.Enqueue(uri);
            Thread.Sleep(grayTTL);
            var w = "";
            grayURIs.TryDequeue(out w);
        }

        protected override ILog Log
        {
            get { return LogManager.GetLogger(typeof(RandomClusterClient)); }
        }
    }
}