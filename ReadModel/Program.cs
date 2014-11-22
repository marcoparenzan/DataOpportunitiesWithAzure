using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System.Configuration;
using Models;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;

namespace ReadModel
{
    class Program
    {
        static void Main(string[] args)
        {
            DocumentClient client = new DocumentClient(
                new Uri(ConfigurationManager.AppSettings["documentdb-EndpointUrl"])
                , ConfigurationManager.AppSettings["documentdb-AuthorizationKey"]
            );
            var databaseName = ConfigurationManager.AppSettings["documentdb-DataBaseId"];
            var all = client.CreateDatabaseQuery().ToList();
            Database database = all.SingleOrDefault();
            var employees = client.CreateDocumentCollectionQuery(database.CollectionsLink).Where(xx => xx.Id == "Employees").ToList().SingleOrDefault();
            var query = client.CreateDocumentQuery<Employee>(employees.DocumentsLink, new FeedOptions
            {
                MaxItemCount = 1000
            }).AsDocumentQuery();
            var task = query.ExecuteNextAsync<Employee>();
            task.Wait();
            var result = task.Result.ToList();

            var storageCredentials = new StorageCredentials(ConfigurationManager.AppSettings["tableStorage-StorageName"], ConfigurationManager.AppSettings["tableStorage-storageKey"]);
            var storageAccount = new CloudStorageAccount(storageCredentials, true);
            var tableClient = storageAccount.CreateCloudTableClient();
            var employessTable = tableClient.GetTableReference("employees");
            employessTable.CreateIfNotExists();

            var i = 0;
            foreach (var e in result)
            {
                var op = TableOperation.Insert(new EmployeeIndexTableEntity{
                    
                    PartitionKey = e.CountryRegionName
                    ,
                    RowKey = e.BusinessEntityID.ToString()
                    ,
                    FirstName = e.FirstName
                    ,
                    LastName = e.LastName
                    ,
                    Title = e.Title
                    ,
                    EMail = e.EmailAddress
                    ,
                    Phone = e.PhoneNumber
                    ,
                    PageNumber = i / 25
                    ,
                    InPageIndex = i % 25 
                });

                i++;
                employessTable.Execute(op);
            }
        }
    }
}
