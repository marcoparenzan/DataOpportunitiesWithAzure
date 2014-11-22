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
    public class RedisQueryablePage<T> : IPage<T>
    {
        public IEnumerable<T> Rows { get; private set; }
        public string Name { get; private set; }
        public int Number { get; private set; }
        public int Size { get; private set; }
        public PageStatus Status { get; private set; }
        public string NextUrl { get; private set; }
        public string PrevUrl { get; private set; }

        public static async Task<IPage<T>> Read(IQueryable<T> query, string name, int pageNumber, int pageSize)
        {
            IDatabase cache = ConnectionMultiplexer.Connect(ConfigurationManager.AppSettings["redis"]).GetDatabase();

            IEnumerable<T> rows = null;
            var pageName = string.Format("{0}-{1}-{2}", name, pageNumber, pageSize);
            var status = PageStatus.Source;
            var pageJson = cache.StringGet(pageName);
            if (pageJson.IsNull)
            {
                rows = query.ToList();
                pageJson = JsonConvert.SerializeObject(rows);
                await cache.StringSetAsync(pageName, pageJson);
            }
            else
            {
                rows = JsonConvert.DeserializeObject<IEnumerable<T>>(pageJson);
                status = PageStatus.Cached;
            }
            return new RedisQueryablePage<T> { 
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