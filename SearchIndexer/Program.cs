using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Models;
using Newtonsoft.Json;

namespace SearchIndexer
{
    static class Program
    {
        private static Uri _serviceUri;
        private static HttpClient _httpClient;

        static void Main(string[] args)
        {
            _serviceUri = new Uri(ConfigurationManager.AppSettings["search-EndpointUrl"]);
            _httpClient = new HttpClient();
            // Get the search service connection information from the App.config
            _httpClient.DefaultRequestHeaders.Add("api-key", ConfigurationManager.AppSettings["search-QueryKey"]);

            DocumentClient client = new DocumentClient(
                new Uri(ConfigurationManager.AppSettings["documentdb-EndpointUrl"])
                , ConfigurationManager.AppSettings["documentdb-AuthorizationKey"]
            );
            var databaseName = ConfigurationManager.AppSettings["documentdb-DataBaseId"];
            var all = client.CreateDatabaseQuery().ToList();
            Database database = all.SingleOrDefault();
            var employees = client.CreateDocumentCollectionQuery(database.CollectionsLink).Where(xx => xx.Id == "Employees").ToList().SingleOrDefault();
            var query = client.CreateDocumentQuery<Employee>(employees.DocumentsLink, new FeedOptions { 
                MaxItemCount = 1000
            }).AsDocumentQuery();
            var task = query.ExecuteNextAsync<Employee>();
            task.Wait();
            var result = task.Result.ToList();
            Uri uri = new Uri(_serviceUri, "/indexes/employeesmain/docs/index");
            HttpResponseMessage response = SendSearchRequest(_httpClient, HttpMethod.Post, uri, JsonConvert.SerializeObject(new
            {
                value = result.Select(xx => new
                {
                    id = xx.BusinessEntityID.ToString()
                    ,
                    firstName = xx.FirstName
                    ,
                    lastName = xx.LastName
                    ,
                    title = xx.Title
                    ,
                    email = xx.EmailAddress
                    ,
                    phone = xx.PhoneNumber

                }).ToArray()
            }));
            response.EnsureSuccessStatusCode();
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
