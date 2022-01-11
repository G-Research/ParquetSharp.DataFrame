using System;
using System.IO;

namespace ParquetSharp.DataFrame.Test
{
    internal sealed class UnitTestDisposableDirectory : IDisposable
    {
        public UnitTestDisposableDirectory()
        {
            var path = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            Info = new DirectoryInfo(path);
            Info.Create();
        }

        public DirectoryInfo Info { get; }

        public void Dispose()
        {
            Info.Delete(true);
        }
    }
}
