using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using ClusterClient.Clients;
using Fclp;
using log4net;
using log4net.Config;

namespace ClusterClient
{
    class Program
    {
        static void Main(string[] args)
        {
            XmlConfigurator.Configure();
            args = new  [] { "-f", "ServerAddresses.txt" };

            string[] replicaAddresses;
            if (!TryGetReplicaAddresses(args, out replicaAddresses))
                return;

            var replicaAverageDelay = new Dictionary<string, Tuple<long, int>>();
            foreach (var address in replicaAddresses)
                replicaAverageDelay.Add(address, Tuple.Create((long)0, 0));

            try
            {
                var clients = new ClusterClientBase[]
                              {
                                  new RandomClusterClient(replicaAddresses),
                                  new GrayListClusterClient(replicaAddresses, TimeSpan.FromMilliseconds(1000)), 
                                  new AllAtOneTimeClusterClient(replicaAddresses),
                                  new RoundRobinClusterClient(replicaAddresses, replicaAverageDelay), 
                                  new SmartClusterClient(replicaAddresses, replicaAverageDelay), 
                              };
                var queries = new[] {"От", "топота", "копыт", "пыль", "по", "полю", "летит", "На", "дворе", "трава", "на", "траве", "дрова"};

                foreach (var client in clients)
                {
                    Console.WriteLine("Testing {0} started", client.GetType());
                    Task.WaitAll(queries.Select(
                        async query =>
                        {
                            var timer = Stopwatch.StartNew();
                            try
                            {
                                var result = await client.ProcessRequestAsync(query, TimeSpan.FromSeconds(6));

                                var processTime = timer.ElapsedMilliseconds;
                                var averDelayAndQueryCount = replicaAverageDelay[result.URI];
                                var newAverDelay = (averDelayAndQueryCount.Item1 * averDelayAndQueryCount.Item2 + processTime) /
                                                   (averDelayAndQueryCount.Item2 + 1);
                                replicaAverageDelay[result.URI] = Tuple.Create(newAverDelay, averDelayAndQueryCount.Item2 + 1);

                                Console.WriteLine("Processed query \"{0}\" in {1} ms", query, processTime);
                            }
                            catch (TimeoutException)
                            {
                                Console.WriteLine("Query \"{0}\" timeout ({1} ms)", query, timer.ElapsedMilliseconds);
                            }
                        }).ToArray());
                    Console.WriteLine("Testing {0} finished", client.GetType());
                }
            }
            catch (Exception e)
            {
                Log.Fatal(e);
            }
        }

        private static bool TryGetReplicaAddresses(string[] args, out string[] replicaAddresses)
        {
            var argumentsParser = new FluentCommandLineParser();
            string[] result = {};

            argumentsParser.Setup<string>('f', "file")
                .WithDescription("Path to the file with replica addresses")
                .Callback(fileName => result = File.ReadAllLines(fileName))
                .Required();

            argumentsParser.SetupHelp("?", "h", "help")
                .Callback(text => Console.WriteLine(text));

            var parsingResult = argumentsParser.Parse(args);

            if (parsingResult.HasErrors)
            {
                argumentsParser.HelpOption.ShowHelp(argumentsParser.Options);
                replicaAddresses = null;
                return false;
            }

            replicaAddresses = result;
            return !parsingResult.HasErrors;
        }

        private static readonly ILog Log = LogManager.GetLogger(typeof(Program));
    }
}
