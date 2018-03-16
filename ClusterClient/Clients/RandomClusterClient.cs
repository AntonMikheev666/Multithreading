using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class RandomClusterClient : ClusterClientBase
    {
        private readonly Random random = new Random();

        public RandomClusterClient(string[] replicaAddresses)
            : base(replicaAddresses)
        {
        }

        public async override Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            //var rands = new ConcurrentBag<int>();
            //while(rands.Count < ReplicaAddresses.Length)
            //{
                var rand = random.Next(ReplicaAddresses.Length);
                var uri = ReplicaAddresses[rand];
                var webRequest = CreateRequest(uri + "?query=" + query);

                Log.InfoFormat("Processing {0}", webRequest.RequestUri);

                var resultTask = ProcessRequestAsync(webRequest);
                await Task.WhenAny(resultTask, Task.Delay(timeout));
                if (!resultTask.IsCompleted)
                    throw new TimeoutException();

                if (resultTask.IsFaulted)
                    return resultTask.Result;
            //    rands.Add(rand);
            //}
            throw new TimeoutException();
        }


        protected override ILog Log
        {
            get { return LogManager.GetLogger(typeof(RandomClusterClient)); }
        }
    }
}