using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class AllAtOneTimeClusterClient : ClusterClientBase
    {

        public AllAtOneTimeClusterClient(string[] replicaAddresses)
            : base(replicaAddresses)
        {
        }

        public async override Task<TaskResult> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var webRequests = ReplicaAddresses
                .ToDictionary(uri => uri, uri => CreateRequest(uri + "?query=" + query));

            foreach (var request in webRequests)
                Log.InfoFormat("Processing {0}", request.Value.RequestUri);

            var requestTasks = webRequests
                .ToDictionary(k => k.Key, async request => await ProcessRequestAsync(request.Value))
                .ToArray();

            var resultTask = await Task.WhenAny(requestTasks.Select(p => p.Value).ToArray());
            await Task.WhenAny(resultTask, Task.Delay(timeout));
            if (!resultTask.IsCompleted)
                throw new TimeoutException();

            
            return new TaskResult(resultTask.Result, requestTasks.First(p => p.Value == resultTask).Key);
        }

        protected override ILog Log
        {
            get { return LogManager.GetLogger(typeof(RandomClusterClient)); }
        }
    }
}