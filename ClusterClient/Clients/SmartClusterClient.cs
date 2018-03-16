using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class SmartClusterClient : ClusterClientBase
    {
        public SmartClusterClient(string[] replicaAddresses)
            : base(replicaAddresses)
        {
        }

        public async override Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            timeout = new TimeSpan((long)(timeout.Ticks * 1.0 / ReplicaAddresses.Length));

            var tasksToAwait = new ConcurrentBag<Task<string>>();

            foreach (var uri in ReplicaAddresses)
            {
                var webRequest = CreateRequest(uri + "?query=" + query);

                Log.InfoFormat("Processing {0}", webRequest.RequestUri);

                
                tasksToAwait.Add(ProcessRequestAsync(webRequest));
                var resultTask = await Task.WhenAny(tasksToAwait);
                Task.WaitAny(resultTask, Task.Delay(timeout));
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