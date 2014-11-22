using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net;
using System.Web;
using System.Web.Mvc;
using System.Threading.Tasks;

using Models;
using System.Configuration;

using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System.Net.Http;
using Newtonsoft.Json;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage;

namespace ClassicWebApp.Controllers
{
    public class EmployeesController : Controller
    {
        public async Task<ActionResult> Index(int? id, int pageNumber = 0, int pageSize = 25)
        {
            var db = new AdventureWorksSmall();
            var query = db.Employees.OrderBy(xx => xx.LastName).ThenBy(xx => xx.FirstName).Skip(pageNumber * pageSize).Take(pageSize);
            var name = "Employees";

            var page = await QueryablePage<Employee>.Read(query, name, pageNumber, pageSize);
            return View(page);
        }

        public async Task<ActionResult> CachedIndex(int? id, int pageNumber = 0, int pageSize = 25)
        {
            var db = new AdventureWorksSmall();
            var query = db.Employees.OrderBy(xx => xx.LastName).ThenBy(xx => xx.FirstName).Skip(pageNumber * pageSize).Take(pageSize);
            var name = "Employees";

            var page = await RedisQueryablePage<Employee>.Read(query, name, pageNumber, pageSize);
            return View("Index", page);
        }

        public async Task<ActionResult> NoSqlIndex(int? id, int pageNumber = 0, int pageSize = 25)
        {
            DocumentClient client = new DocumentClient(
                new Uri(ConfigurationManager.AppSettings["documentdb-EndpointUrl"])
                , ConfigurationManager.AppSettings["documentdb-AuthorizationKey"]
            );
            var databaseName = ConfigurationManager.AppSettings["documentdb-DataBaseId"];
            var all = client.CreateDatabaseQuery().ToList();
            Database database = all.SingleOrDefault();
            var employees = client.CreateDocumentCollectionQuery(database.CollectionsLink).Where(xx => xx.Id == "Employees").ToList().SingleOrDefault();
            var query = client.CreateDocumentQuery<Employee>(employees.DocumentsLink, new FeedOptions { 
                MaxItemCount = pageSize
            }).AsDocumentQuery();
            var page = await DocumentQueryablePage<Employee>.Read(query, "NoSqlEmployees", pageNumber, pageSize);
            return View("Index", page);
        }

        public ActionResult SearchIndex()
        {
            return View();
        }

        private Uri _serviceUri;
        private HttpClient _httpClient;

        public async Task<ContentResult> SearchEmployees(string id)
        {
            _serviceUri = new Uri(ConfigurationManager.AppSettings["search-EndpointUrl"]);
            _httpClient = new HttpClient();
            // Get the search service connection information from the App.config
            _httpClient.DefaultRequestHeaders.Add("api-key", ConfigurationManager.AppSettings["search-QueryKey"]);
            Uri uri = new Uri(_serviceUri, "/indexes/employeesmain/docs?search=" + id);
            HttpResponseMessage response = SendSearchRequest(_httpClient, HttpMethod.Get, uri);
            return Content(await response.Content.ReadAsStringAsync(), "application/json");
        }
        private HttpResponseMessage SendSearchRequest(HttpClient client, HttpMethod method, Uri uri)
        {
            UriBuilder builder = new UriBuilder(uri);
            string separator = string.IsNullOrWhiteSpace(builder.Query) ? string.Empty : "&";
            builder.Query = builder.Query.TrimStart('?') + separator + "api-version=2014-07-31-Preview";

            var request = new HttpRequestMessage(method, builder.Uri);
            return client.SendAsync(request).Result;
        }

        public async Task<ActionResult> TableStorageIndex(int? id, int pageNumber = 0, int pageSize = 25)
        {
            var storageCredentials = new StorageCredentials(ConfigurationManager.AppSettings["tableStorage-StorageName"], ConfigurationManager.AppSettings["tableStorage-storageKey"]);
            var storageAccount = new CloudStorageAccount(storageCredentials, true);
            var tableClient = storageAccount.CreateCloudTableClient();
            var employessTable = tableClient.GetTableReference("employees");

            var query = employessTable.CreateQuery<EmployeeIndexTableEntity>().Where(xx => xx.PageNumber == pageNumber).Select(xx => new Employee{
            
                FirstName = xx.FirstName
                ,
                LastName = xx.LastName
                ,
                Title = xx.Title
                ,
                EmailAddress = xx.EMail
                ,
                PhoneNumber = xx.Phone
            });

            var page = await RedisQueryablePage<Employee>.Read(query, "TableStorageEmployees", pageNumber, pageSize);
            return View("Index", page);
        }
    }
}
