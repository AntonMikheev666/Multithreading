using System;
using System.Collections.Generic;
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

            var tasksToAwait = new List<Task<string>>();

            foreach (var uri in ReplicaAddresses)
            {
                var webRequest = CreateRequest(uri + "?query=" + query);

                Log.InfoFormat("Processing {0}", webRequest.RequestUri);

                
                tasksToAwait.Add(ProcessRequestAsync(webRequest));
                var resultTask = await Task.WhenAny(tasksToAwait);
                await Task.WhenAny(resultTask, Task.Delay(timeout));
                if (resultTask.IsCompleted)
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