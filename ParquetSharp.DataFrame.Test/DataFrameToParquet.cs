using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Data.Analysis;
using Xunit;

namespace ParquetSharp.DataFrame.Test
{
    public class DataFrameToParquet
    {
        [Fact]
        public void TestToParquet()
        {
            int numRows = 10_000;
            var testColumns = GetTestColumns();
            var columns = new List<DataFrameColumn>(testColumns.Length);
            foreach (var testCol in testColumns)
            {
                columns.Add(testCol.GetColumn(numRows));
            }

            var dataFrame = new Microsoft.Data.Analysis.DataFrame(columns);

            using var dir = new UnitTestDisposableDirectory();
            var filePath = Path.Join(dir.Info.FullName, "test.parquet");
            dataFrame.ToParquet(filePath);

            Assert.True(File.Exists(filePath));

            using var fileReader = new ParquetFileReader(filePath);
            Assert.Equal(testColumns.Length, fileReader.FileMetaData.NumColumns);
            Assert.Equal(numRows, fileReader.FileMetaData.NumRows);

            long offset = 0;
            for (var rowGroupIdx = 0; rowGroupIdx < fileReader.FileMetaData.NumRowGroups; ++rowGroupIdx)
            {
                using var rowGroupReader = fileReader.RowGroup(rowGroupIdx);
                int colIdx = 0;
                foreach (var testCol in testColumns)
                {
                    using var parquetCol = rowGroupReader.Column(colIdx++);
                    using var logicalReader = parquetCol.LogicalReader();
                    testCol.VerifyData(logicalReader, offset);
                }
                offset += rowGroupReader.MetaData.NumRows;
            }
        }

        private readonly struct TestColumn
        {
            public Func<int, DataFrameColumn> GetColumn { get; init; }

            public Action<LogicalColumnReader, long> VerifyData { get; init; }
        }

        private static TestColumn[] GetTestColumns()
        {
            return new[]
            {
                new TestColumn
                {
                    GetColumn = numRows =>
                        new PrimitiveDataFrameColumn<int>("int", Enumerable.Range(0, numRows).Select(i => i)),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<int>, offset, (i, elem) => Assert.Equal(i, elem)),
                },
            };
        }

        private static void VerifyData<TElement>(
            LogicalColumnReader<TElement> columnReader, long offset, Action<long, TElement> verifier)
        {
            var buffer = new TElement[columnReader.BufferLength];
            long readerOffset = 0;
            while (columnReader.HasNext)
            {
                var read = columnReader.ReadBatch((Span<TElement>) buffer);
                for (int i = 0; i < read; ++i)
                {
                    verifier(offset + readerOffset + i, buffer[i]);
                }
                readerOffset += read;
            }
        }
    }
}
