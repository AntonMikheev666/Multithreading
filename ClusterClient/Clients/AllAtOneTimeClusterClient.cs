using System;
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
                .Select(uri => CreateRequest(uri + "?query=" + query));

            foreach (var request in webRequests)
                Log.InfoFormat("Processing {0}", request.RequestUri);

            var resultTasks = webRequests
                .Select(async request => await ProcessRequestAsync(request))
                .ToArray();

            var result = await Task.WhenAny(resultTasks);
            await Task.WhenAny(result, Task.Delay(timeout));
            if (!result.IsCompleted)
                throw new TimeoutException();

            return result.Result;
        }

        protected override ILog Log
        {
            get { return LogManager.GetLogger(typeof(RandomClusterClient)); }
        }
    }
}