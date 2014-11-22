using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace CommandInDomain
{
    class Program
    {
        static void Main(string[] args)
        {
            // Send();
            Receive();
        }

        static void Send()
        {
            var factory = MessagingFactory.CreateFromConnectionString(ConfigurationManager.AppSettings["eventHub"]);
            var client = factory.CreateEventHubClient("employees");
            var eventData = new EventData();
            eventData.Properties.Add("firstName", "Marco");
            eventData.Properties.Add("lastName", "Parenzan");
            eventData.Properties.Add("title", "Mr");
            eventData.Properties.Add("email", "marco.parenzan@live.it");
            eventData.Properties.Add("phone", "393xxxxxx");
            client.Send(eventData);
        }

        static void Receive()
        {
            // docDB
            DocumentClient docClient = new DocumentClient(
                new Uri(ConfigurationManager.AppSettings["documentdb-EndpointUrl"])
                , ConfigurationManager.AppSettings["documentdb-AuthorizationKey"]
            );
            var databaseName = ConfigurationManager.AppSettings["documentdb-DataBaseId"];
            Database database = docClient.CreateDatabaseQuery().Where(xx => xx.Id == databaseName).ToList().SingleOrDefault();
            var employees = docClient.CreateDocumentCollectionQuery(database.CollectionsLink).Where(xx => xx.Id == "Employees").ToList().SingleOrDefault();

            // event
            var ns = NamespaceManager.CreateFromConnectionString(ConfigurationManager.AppSettings["eventHub"]);
            var eventHub = ns.GetEventHub("employees");

            var factory = MessagingFactory.CreateFromConnectionString(ConfigurationManager.AppSettings["eventHub"]);
            var client = factory.CreateEventHubClient("employees");
            var group = client.GetDefaultConsumerGroup();

            // run a task per partition
            var tasks = new List<Task>();
            foreach (var partitionId in eventHub.PartitionIds)
            {
                tasks.Add(Task.Run(() =>
                {
                    var receiver = group.CreateReceiver(partitionId);
                    var data = receiver.Receive();
                    if (data != null)
                    {
                        //receive message and handle it!
                        var e = new Employee
                        { 
                            FirstName = data.Properties["firstName"].ToString()
                            , 
                            LastName = data.Properties["lastName"].ToString()
                            ,
                            Title = data.Properties["title"].ToString()
                            ,
                            EmailAddress = data.Properties["email"].ToString()
                            ,
                            PhoneNumber = data.Properties["phone"].ToString()
                        };

                        // insert as a document
                        var task = docClient.CreateDocumentAsync(employees.SelfLink, e);
                        task.Wait();

                        // update index
                        var _serviceUri = new Uri(ConfigurationManager.AppSettings["search-EndpointUrl"]);
                        var _httpClient = new HttpClient();
                        // Get the search service connection information from the App.config
                        _httpClient.DefaultRequestHeaders.Add("api-key", ConfigurationManager.AppSettings["search-QueryKey"]);
                        Uri uri = new Uri(_serviceUri, "/indexes/employeesmain/docs/index");
                        HttpResponseMessage response = SendSearchRequest(_httpClient, HttpMethod.Post, uri, JsonConvert.SerializeObject(new
                        {
                            value = new [] { new
                            {
                                id = "1"
                                ,
                                firstName = e.FirstName
                                ,
                                lastName = e.LastName
                                ,
                                title = e.Title
                                ,
                                email = e.EmailAddress
                                ,
                                phone = e.PhoneNumber

                            }}
                        }));
                    }
                    receiver.Close();
                }));
            }

            Task.WaitAll(tasks.ToArray());
        }


        private static HttpResponseMessage SendSearchRequest(HttpClient client, HttpMethod method, Uri uri, string json = null)
        {
            UriBuilder builder = new UriBuilder(uri);
            string separator = string.IsNullOrWhiteSpace(builder.Query) ? string.Empty : "&";
            builder.Query = builder.Query.TrimStart('?') + separator + "api-version=2014-07-31-Preview";

            var request = new HttpRequestMessage(method, builder.Uri);

            if (json != null)
            {
                request.Content = new StringContent(json, Encoding.UTF8, "application/json");
            }

            return client.SendAsync(request).Result;
        }
    }
}
