using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System;
using System.Collections.Generic;
using System.Net.WebSockets;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Gremlin.Net;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Driver.Remote;
using Gremlin.Net.Process;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;
using static Gremlin.Net.Process.Traversal.AnonymousTraversalSource;
using static Gremlin.Net.Process.Traversal.__;
using static Gremlin.Net.Process.Traversal.P;
using static Gremlin.Net.Process.Traversal.Order;
using static Gremlin.Net.Process.Traversal.Operator;
using static Gremlin.Net.Process.Traversal.Pop;
using static Gremlin.Net.Process.Traversal.Scope;
using static Gremlin.Net.Process.Traversal.TextP;
using static Gremlin.Net.Process.Traversal.Column;
using static Gremlin.Net.Process.Traversal.Direction;
using static Gremlin.Net.Process.Traversal.T;
using Gremlin.Net.Structure.IO.GraphSON;
using Newtonsoft.Json;

using Amazon.Lambda.Core;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace neptunedotnetsigv4ws
{
    public class Function
    {

        /// <summary>
        /// A simple function that takes a string and does a ToUpper
        /// </summary>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        /// 
        private  Dictionary<string, string> gremlinQueries = new Dictionary<string, string>
        {
            { "Cleanup",        "g.V().drop()" },
            { "AddVertex 1",    "g.addV('person').property('id', 'thomas').property('firstName', 'Thomas').property('age', 44)" },
            { "AddVertex 2",    "g.addV('person').property('id', 'mary').property('firstName', 'Mary').property('lastName', 'Andersen').property('age', 39)" },
            { "AddVertex 3",    "g.addV('person').property('id', 'ben').property('firstName', 'Ben').property('lastName', 'Miller')" },
            { "AddVertex 4",    "g.addV('person').property('id', 'robin').property('firstName', 'Robin').property('lastName', 'Wakefield')" },
            { "AddEdge 1",      "g.V('thomas').addE('knows').to(g.V('mary'))" },
            { "AddEdge 2",      "g.V('thomas').addE('knows').to(g.V('ben'))" },
            { "AddEdge 3",      "g.V('ben').addE('knows').to(g.V('robin'))" },
            { "UpdateVertex",   "g.V('thomas').property('age', 44)" },
            { "CountVertices",  "g.V().count()" },
            { "Filter Range",   "g.V().hasLabel('person').has('age', gt(40))" },
            { "Project",        "g.V().hasLabel('person').values('firstName')" },
            { "Sort",           "g.V().hasLabel('person').order().by('firstName', decr)" },
            { "Traverse",       "g.V('thomas').out('knows').hasLabel('person')" },
            { "Traverse 2x",    "g.V('thomas').out('knows').hasLabel('person').out('knows').hasLabel('person')" },
            { "Loop",           "g.V('thomas').repeat(out()).until(has('id', 'robin')).path()" },
            { "DropEdge",       "g.V('thomas').outE('knows').where(inV().has('id', 'mary')).drop()" },
            { "CountEdges",     "g.E().count()" },
            { "DropVertex",     "g.V('thomas').drop()" },
        };

        public string FunctionHandler(object input, ILambdaContext context)
        {
            var access_key = "youraccesskey";
            var secret_key = "yoursecretkey";
            var neptune_endpoint = "yourneptuneendpoint:8182"; // ex: mycluster.cluster.us-east-1.neptune.amazonaws.com
            var neptune_region = "us-east-1"; //ex: us-east-1
            var using_ELB = false; //Set to True if using and ELB and define ELB URL via ELB_endpoint variable
            var ELB_endpoint = ""; //ex: myelb.elb.us-east-1.amazonaws.com

            var neptunesigner = new AWS4RequestSigner(access_key, secret_key);
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
                RequestUri = new Uri("http://" + neptune_endpoint + "/gremlin")
            };
            var signedrequest = neptunesigner.Sign(request, "neptune-db", neptune_region);
            var authText = signedrequest.Headers.GetValues("Authorization").FirstOrDefault();
            var authDate = signedrequest.Headers.GetValues("x-amz-date").FirstOrDefault();

            var webSocketConfiguration = new Action<ClientWebSocketOptions>(options => {
                options.SetRequestHeader("host", neptune_endpoint);
                options.SetRequestHeader("x-amz-date", authDate);
                options.SetRequestHeader("Authorization", authText);
            });

            /* GremlinServer() accepts the hostname and port as separate parameters.  
             * Split the endpoint into both variables to pass to GremlinServer()
             *
             * Also - determine if an ELB is used.  If using an ELB, connect using the ELB hostname.
             */

            var neptune_host = "";
            if (using_ELB)
            {
                neptune_host = ELB_endpoint;
            }
            else
            {
                neptune_host = neptune_endpoint.Split(':')[0];
            }

            var neptune_port = int.Parse(neptune_endpoint.Split(':')[1]);

            var gremlinServer = new GremlinServer(neptune_host, neptune_port);


            var gremlinClient = new GremlinClient(gremlinServer, webSocketConfiguration: webSocketConfiguration);
            var remoteConnection = new DriverRemoteConnection(gremlinClient);
            var g = Traversal().WithRemote(remoteConnection);

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

                
              
               

            }

            return "Hello World";
        }

        private  Task<ResultSet<dynamic>> SubmitRequest(GremlinClient gremlinClient, KeyValuePair<string, string> query)
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

               
               
                throw;
            }
        }

       

        public  string GetValueAsString(IReadOnlyDictionary<string, object> dictionary, string key)
        {
            return JsonConvert.SerializeObject(GetValueOrDefault(dictionary, key));
        }

        public  object GetValueOrDefault(IReadOnlyDictionary<string, object> dictionary, string key)
        {
            if (dictionary.ContainsKey(key))
            {
                return dictionary[key];
            }

            return null;
        }
    }
}
