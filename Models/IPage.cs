using System;
using System.Collections.Generic;

namespace Models
{
    public interface IPage<T>
    {
        string Name { get; }
        int Number { get; }
        IEnumerable<T> Rows { get; }
        int Size { get; }
        PageStatus Status { get; }
        string NextUrl { get; }
        string PrevUrl { get; }
    }
}
