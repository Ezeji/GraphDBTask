using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Structure.IO.GraphSON;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net.WebSockets;
using System.Threading.Tasks;

namespace BetaQuickTask
{
    /// <summary>
    /// Sample program that shows the use of Graph (Gremlin) APIs for Azure Cosmos DB using the open-source connector Gremlin.Net
    /// </summary>
    class Program
    {
        // Azure Cosmos DB Configuration variables
        // Replace the values in these variables to your own.
        // <configureConnectivity>
        private static string host = "test-azure-cosmodb.gremlin.cosmos.azure.com";
        private static string primaryKey = "lYuosIm6LgBFFpb0b8JsTVwIkzNicMNznqyjZ4v0F3mA7mnWLawA9FjyBaMLLrN6qRlb62AiUc6f3H4b3UQtsA==";
        private static string database = "betaquicktask-database";
        private static string container = "betaquicktask-graph";

        private static bool EnableSSL
        {
            get
            {
                if (Environment.GetEnvironmentVariable("EnableSSL") == null)
                {
                    return true;
                }

                if (!bool.TryParse(Environment.GetEnvironmentVariable("EnableSSL"), out bool value))
                {
                    throw new ArgumentException("Invalid env var: EnableSSL is not a boolean");
                }

                return value;
            }
        }

        private static int Port
        {
            get
            {
                if (Environment.GetEnvironmentVariable("Port") == null)
                {
                    return 443;
                }

                if (!int.TryParse(Environment.GetEnvironmentVariable("Port"), out int port))
                {
                    throw new ArgumentException("Invalid env var: Port is not an integer");
                }

                return port;
            }
        }

        // </configureConnectivity>

        // Gremlin queries that will be executed.
        // <defineQueries>
        private static Dictionary<string, string> gremlinQueries = new Dictionary<string, string>
        {
            // This query creates the route information in an Azure CosmosDB Gremlin Database.

            { "Cleanup",        "g.V().drop()" },
            { "AddVertex 1",    "g.addV('city1').property('departureCity', 'City 1').property('arrivalCity', 'City 2').property('departureTime', '7 AM').property('arrivalTime', '8 AM').property('pk', 'pk')" },
            { "AddVertex 2",    "g.addV('city2').property('departureCity', 'City 1').property('arrivalCity', 'City 2').property('departureTime', '7 AM').property('arrivalTime', '8 AM').property('pk', 'pk')" },
            { "AddVertex 3",    "g.addV('city2').property('departureCity', 'City 2').property('arrivalCity', 'City 3').property('departureTime', '5 AM').property('arrivalTime', '6 AM').property('pk', 'pk')" },
            { "AddVertex 4",    "g.addV('city3').property('departureCity', 'City 2').property('arrivalCity', 'City 3').property('departureTime', '5 AM').property('arrivalTime', '6 AM').property('pk', 'pk')" },
            { "AddVertex 5",    "g.addV('city3').property('departureCity', 'City 3').property('arrivalCity', 'City 4').property('departureTime', '9 AM').property('arrivalTime', '10 AM').property('pk', 'pk')" },
            { "AddVertex 6",    "g.addV('city3').property('departureCity', 'City 3').property('arrivalCity', 'City 5').property('departureTime', '5 PM').property('arrivalTime', '6 PM').property('pk', 'pk')" },
            { "AddVertex 7",    "g.addV('city3').property('departureCity', 'City 3').property('arrivalCity', 'City 6').property('departureTime', '3 PM').property('arrivalTime', '5 PM').property('pk', 'pk')" },
            { "AddVertex 8",    "g.addV('city4').property('departureCity', 'City 3').property('arrivalCity', 'City 4').property('departureTime', '9 AM').property('arrivalTime', '10 AM').property('pk', 'pk')" },
            { "AddVertex 9",    "g.addV('city5').property('departureCity', 'City 3').property('arrivalCity', 'City 5').property('departureTime', '5 PM').property('arrivalTime', '6 PM').property('pk', 'pk')" },
            { "AddVertex 10",    "g.addV('city6').property('departureCity', 'City 3').property('arrivalCity', 'City 6').property('departureTime', '3 PM').property('arrivalTime', '5 PM').property('pk', 'pk')" },
            { "AddVertex 11",    "g.addV('city7').property('departureCity', 'City 7').property('arrivalCity', 'City 8').property('departureTime', '7 PM').property('arrivalTime', '8 PM').property('pk', 'pk')" },
            { "AddVertex 12",    "g.addV('city7').property('departureCity', 'City 8').property('arrivalCity', 'City 7').property('departureTime', '9 PM').property('arrivalTime', '10 PM').property('pk', 'pk')" },
            { "AddVertex 13",    "g.addV('city7').property('departureCity', 'City 7').property('arrivalCity', 'City 9').property('departureTime', '11 AM').property('arrivalTime', '11:45 AM').property('pk', 'pk')" },
            { "AddVertex 14",    "g.addV('city8').property('departureCity', 'City 7').property('arrivalCity', 'City 8').property('departureTime', '7 PM').property('arrivalTime', '8 PM').property('pk', 'pk')" },
            { "AddVertex 15",    "g.addV('city8').property('departureCity', 'City 8').property('arrivalCity', 'City 7').property('departureTime', '9 PM').property('arrivalTime', '10 PM').property('pk', 'pk')" },
            { "AddVertex 16",    "g.addV('city9').property('departureCity', 'City 7').property('arrivalCity', 'City 9').property('departureTime', '11 AM').property('arrivalTime', '11:45 AM').property('pk', 'pk')" },
            { "AddVertex 17",    "g.addV('city9').property('departureCity', 'City 9').property('arrivalCity', 'City 10').property('departureTime', '2 PM').property('arrivalTime', '3 PM').property('pk', 'pk')" },
            { "AddVertex 18",    "g.addV('city10').property('departureCity', 'City 9').property('arrivalCity', 'City 10').property('departureTime', '2 PM').property('arrivalTime', '3 PM').property('pk', 'pk')" },

            /* This query performs count of adjacent route connections for each city
             * using their respective database labels such as city1, city2, city3.etc
             */
            { "CountVertices",  "g.V().hasLabel('city1').count()" },

        };
        // </defineQueries>

        // Starts a console application that executes every Gremlin query in the gremlinQueries dictionary. 
        static void Main(string[] args)
        {
            // <defineClientandServerObjects>
            string containerLink = "/dbs/" + database + "/colls/" + container;
            Console.WriteLine($"Connecting to: host: {host}, port: {Port}, container: {containerLink}, ssl: {EnableSSL}");
            var gremlinServer = new GremlinServer(host, Port, enableSsl: EnableSSL,
                                                    username: containerLink,
                                                    password: primaryKey);

            ConnectionPoolSettings connectionPoolSettings = new ConnectionPoolSettings()
            {
                MaxInProcessPerConnection = 10,
                PoolSize = 30,
                ReconnectionAttempts = 3,
                ReconnectionBaseDelay = TimeSpan.FromMilliseconds(500)
            };

            var webSocketConfiguration =
                new Action<ClientWebSocketOptions>(options =>
                {
                    options.KeepAliveInterval = TimeSpan.FromSeconds(10);
                });


            using (var gremlinClient = new GremlinClient(
                gremlinServer,
                new GraphSON2Reader(),
                new GraphSON2Writer(),
                GremlinClient.GraphSON2MimeType,
                connectionPoolSettings,
                webSocketConfiguration))
            {
                // </defineClientandServerObjects>

                // <executeQueries>
                foreach (var query in gremlinQueries)
                {
                    Console.WriteLine(String.Format("Running this query: {0}: {1}", query.Key, query.Value));

                    // Create async task to execute the Gremlin query.
                    var resultSet = SubmitRequest(gremlinClient, query).Result;
                    if (resultSet.Count > 0)
                    {
                        Console.WriteLine("\tResult:");
                        foreach (var result in resultSet)
                        {
                            // The vertex results are formed as Dictionaries with a nested dictionary for their properties
                            string output = JsonConvert.SerializeObject(result);
                            Console.WriteLine($"\t{output}");
                        }
                        Console.WriteLine();
                    }

                    // Print the status attributes for the result set.
                    // This includes the following:
                    //  x-ms-status-code            : This is the sub-status code which is specific to Cosmos DB.
                    //  x-ms-total-request-charge   : The total request units charged for processing a request.
                    //  x-ms-total-server-time-ms   : The total time executing processing the request on the server.
                    PrintStatusAttributes(resultSet.StatusAttributes);
                    Console.WriteLine();
                }
                // </executeQueries>
            }

            // Exit program
            Console.WriteLine("Done. Press any key to exit...");
            Console.ReadLine();
        }

        private static Task<ResultSet<dynamic>> SubmitRequest(GremlinClient gremlinClient, KeyValuePair<string, string> query)
        {
            try
            {
                return gremlinClient.SubmitAsync<dynamic>(query.Value);
            }
            catch (ResponseException e)
            {
                Console.WriteLine("\tRequest Error!");

                // Print the Gremlin status code.
                Console.WriteLine($"\tStatusCode: {e.StatusCode}");

                // On error, ResponseException.StatusAttributes will include the common StatusAttributes for successful requests, as well as
                // additional attributes for retry handling and diagnostics.
                // These include:
                //  x-ms-retry-after-ms         : The number of milliseconds to wait to retry the operation after an initial operation was throttled. This will be populated when
                //                              : attribute 'x-ms-status-code' returns 429.
                //  x-ms-activity-id            : Represents a unique identifier for the operation. Commonly used for troubleshooting purposes.
                PrintStatusAttributes(e.StatusAttributes);
                Console.WriteLine($"\t[\"x-ms-retry-after-ms\"] : { GetValueAsString(e.StatusAttributes, "x-ms-retry-after-ms")}");
                Console.WriteLine($"\t[\"x-ms-activity-id\"] : { GetValueAsString(e.StatusAttributes, "x-ms-activity-id")}");

                throw;
            }
        }

        private static void PrintStatusAttributes(IReadOnlyDictionary<string, object> attributes)
        {
            Console.WriteLine($"\tStatusAttributes:");
            Console.WriteLine($"\t[\"x-ms-status-code\"] : { GetValueAsString(attributes, "x-ms-status-code")}");
            Console.WriteLine($"\t[\"x-ms-total-server-time-ms\"] : { GetValueAsString(attributes, "x-ms-total-server-time-ms")}");
            Console.WriteLine($"\t[\"x-ms-total-request-charge\"] : { GetValueAsString(attributes, "x-ms-total-request-charge")}");
        }

        public static string GetValueAsString(IReadOnlyDictionary<string, object> dictionary, string key)
        {
            return JsonConvert.SerializeObject(GetValueOrDefault(dictionary, key));
        }

        public static object GetValueOrDefault(IReadOnlyDictionary<string, object> dictionary, string key)
        {
            if (dictionary.ContainsKey(key))
            {
                return dictionary[key];
            }

            return null;
        }
    }
}
