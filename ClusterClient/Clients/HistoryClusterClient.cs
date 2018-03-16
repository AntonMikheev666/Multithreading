using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class HistoryClusterClient : ClusterClientBase
    {
        private readonly ConcurrentDictionary<string, Tuple<long, int>> replicaAverageDelay;

        public HistoryClusterClient(string[] replicaAddresses)
            : base(replicaAddresses)
        {
            replicaAverageDelay = new ConcurrentDictionary<string, Tuple<long, int>>();
            foreach (var address in replicaAddresses)
                replicaAverageDelay.TryAdd(address, Tuple.Create((long)0, 0));
        }

        public async override Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var timer = Stopwatch.StartNew();

            Tuple<long, int> valueForLinq;
            var webRequests = ReplicaAddresses
                .OrderBy(uri =>
                {
                    replicaAverageDelay.TryGetValue(uri, out valueForLinq);
                    return valueForLinq.Item1;
                })
                .ToDictionary(uri => uri, uri => CreateRequest(uri + "?query=" + query));

            timeout = new TimeSpan((long)(timeout.Ticks * 1.0 / ReplicaAddresses.Length));

            var tasksToAwait = new ConcurrentBag<Task<string>>();

            foreach (var pair in webRequests)
            {
                var tempPair = pair;
                Log.InfoFormat("Processing {0}", tempPair.Value.RequestUri);

                tasksToAwait.Add(ProcessRequestAsync(tempPair.Value));
                var resultTask = await Task.WhenAny(tasksToAwait);
                Task.WaitAny(resultTask, Task.Delay(timeout));

                if (!resultTask.IsCompleted)
                    continue;

                if (!resultTask.IsFaulted)
                {
                    var processTime = timer.ElapsedMilliseconds;

                    Tuple<long, int> currentValue;
                    replicaAverageDelay.TryGetValue(tempPair.Key, out currentValue);

                    var newAverDelay = (currentValue.Item1 * currentValue.Item2 + processTime) /
                                       (currentValue.Item2 + 1);

                    replicaAverageDelay.TryUpdate(tempPair.Key,
                        Tuple.Create(newAverDelay, currentValue.Item2 + 1), currentValue);

                    return resultTask.Result;
                }
            }

            throw new TimeoutException();
        }

        protected override ILog Log
        {
            get { return LogManager.GetLogger(typeof(RandomClusterClient)); }
        }
    }
}