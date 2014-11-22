using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Models
{
    public class EmployeeIndexTableEntity: TableEntity
    {
        public string FirstName {get;set;}
        public string LastName { get; set; }
        public string Title { get; set; }
        public string EMail { get; set; }
        public string Phone { get; set; }
        public int PageNumber { get; set; }
        public int InPageIndex { get; set; }
    }
}
