using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Models;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FromSQLtoNoSQL
{
    class Program
    {
        static void Main(string[] args)
        {
            Do();
        }

        static async void Do()
        {
            DocumentClient client = new DocumentClient(
                new Uri(ConfigurationManager.AppSettings["documentdb-EndpointUrl"])
                , ConfigurationManager.AppSettings["documentdb-AuthorizationKey"]
                );
            var databaseName = ConfigurationManager.AppSettings["documentdb-DataBaseId"];
            var all = client.CreateDatabaseQuery().ToList();
            Database database = all.SingleOrDefault();
            if (database == null)
            {
                database = await client.CreateDatabaseAsync(
                    new Database
                    {
                        Id = databaseName
                    }, new RequestOptions { });
            }
            var employees = client.CreateDocumentCollectionQuery(database.CollectionsLink).Where(xx => xx.Id == "Employees").ToList().SingleOrDefault();
            if (employees == null)
            {
                employees = await client.CreateDocumentCollectionAsync(
                    database.SelfLink
                    , new DocumentCollection
                    {
                        Id = "Employees"
                    });
            }

            var db = new AdventureWorksSmall();
            foreach (var e in db.Employees.OrderBy(xx => xx.LastName).ThenBy(xx => xx.FirstName))
            {
                var task = client.CreateDocumentAsync(employees.SelfLink, e, new RequestOptions { 
                
                     
                
                });
                task.Wait();
            }
        }
    }
}