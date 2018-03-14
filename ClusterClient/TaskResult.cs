namespace ClusterClient.Clients
{
    public class TaskResult
    {
        public string QueryResult { get; }
        public string URI { get; }

        public TaskResult(string queryResult, string uri)
        {
            QueryResult = queryResult;
            URI = uri;
        }
    }
}