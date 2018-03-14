using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class SmartClusterClient : ClusterClientBase
    {
        private readonly Dictionary<string, Tuple<long, int>> replicaAverageDelay;

        public SmartClusterClient(string[] replicaAddresses, Dictionary<string, Tuple<long, int>> replicaAverageDelay)
            : base(replicaAddresses)
        {
            this.replicaAverageDelay = replicaAverageDelay;
        }

        public async override Task<TaskResult> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            timeout = new TimeSpan((long)(timeout.Ticks * 1.0 / ReplicaAddresses.Length));

            var tasksToAwait = new List<Task<string>>();

            foreach (var uri in ReplicaAddresses.OrderBy(u => replicaAverageDelay[u].Item1))
            {
                if(replicaAverageDelay[uri].Item1 == 0)
                    continue;

                var url = uri;
                var webRequest = CreateRequest(uri + "?query=" + query);

                Log.InfoFormat("Processing {0}", webRequest.RequestUri);

                
                tasksToAwait.Add(ProcessRequestAsync(webRequest));
                var resultTask = await Task.WhenAny(tasksToAwait);
                await Task.WhenAny(resultTask, Task.Delay(timeout));
                if (resultTask.IsCompleted)
                    return new TaskResult(resultTask.Result, url);
            }

            throw new TimeoutException();
        }

        protected override ILog Log
        {
            get { return LogManager.GetLogger(typeof(RandomClusterClient)); }
        }
    }
}