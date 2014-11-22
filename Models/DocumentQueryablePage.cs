using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;
using System.Web;

namespace Models
{
    public class DocumentQueryablePage<T> : IPage<T>
    {
        public IEnumerable<T> Rows { get; private set; }
        public string Name { get; private set; }
        public int Number { get; private set; }
        public int Size { get; private set; }
        public PageStatus Status { get; private set; }
        public string NextUrl { get; private set; }
        public string PrevUrl { get; private set; }

        public async static Task<IPage<T>> Read(IDocumentQuery<T> query, string name, int pageNumber, int pageSize)
        {
            IEnumerable<T> rows = null;
            var pageName = string.Format("{0}-{1}-{2}", name, pageNumber, pageSize);
            var status = PageStatus.Source;
            rows = await query.ExecuteNextAsync<T>();
            return new DocumentQueryablePage<T>
            { 
                Rows = rows
                ,
                Name = name
                ,
                Number = pageNumber
                ,
                Size = pageSize
                ,
                Status = status
            };
        }
    }
}