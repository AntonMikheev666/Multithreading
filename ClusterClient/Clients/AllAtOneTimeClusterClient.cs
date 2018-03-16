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

        public async override Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var webRequests = ReplicaAddresses
                .Select(uri => CreateRequest(uri + "?query=" + query))
                .ToArray();

            foreach (var request in webRequests)
                Log.InfoFormat("Processing {0}", request);

            var requestTasks = webRequests
                .Select(async request => await ProcessRequestAsync(request))
                .ToList();
            
            while (requestTasks.Count > 0)
            {
                var resultTask = await Task.WhenAny(requestTasks);
                await Task.WhenAny(resultTask, Task.Delay(timeout));

                if (!resultTask.IsCompleted)
                    throw new TimeoutException();

                if (!resultTask.IsFaulted)
                    return resultTask.Result;

                requestTasks.Remove(resultTask);
            }

            throw new TimeoutException();
        }

        protected override ILog Log
        {
            get { return LogManager.GetLogger(typeof(RandomClusterClient)); }
        }
    }
}