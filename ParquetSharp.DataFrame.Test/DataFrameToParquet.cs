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
                new TestColumn
                {
                    GetColumn = numRows =>
                        new PrimitiveDataFrameColumn<int>("nullable_int", Enumerable.Range(0, numRows).Select(i => i % 10 == 0 ? (int?) null : i)),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<int?>, offset, (i, elem) => Assert.Equal(i % 10 == 0 ? null : (int?) i, elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new PrimitiveDataFrameColumn<long>("long", Enumerable.Range(0, numRows).Select(i => (long) i)),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<long>, offset, Assert.Equal),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new PrimitiveDataFrameColumn<long>("nullable_long", Enumerable.Range(0, numRows).Select(i => i % 10 == 0 ? (long?) null : i)),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<long?>, offset, (i, elem) => Assert.Equal(i % 10 == 0 ? null : i, elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new PrimitiveDataFrameColumn<float>("float", Enumerable.Range(0, numRows).Select(i => (float) i)),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<float>, offset, (i, elem) => Assert.Equal(i, elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new PrimitiveDataFrameColumn<float>("nullable_float", Enumerable.Range(0, numRows).Select(i => i % 10 == 0 ? null : (float?) i)),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<float?>, offset, (i, elem) => Assert.Equal(i % 10 == 0 ? null : (float?) i, elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new PrimitiveDataFrameColumn<double>("double", Enumerable.Range(0, numRows).Select(i => (double) i)),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<double>, offset, (i, elem) => Assert.Equal(i, elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new PrimitiveDataFrameColumn<double>("nullable_double", Enumerable.Range(0, numRows).Select(i => i % 10 == 0 ? null : (double?) i)),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<double?>, offset, (i, elem) => Assert.Equal(i % 10 == 0 ? null : (double?) i, elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new PrimitiveDataFrameColumn<bool>("bool", Enumerable.Range(0, numRows).Select(i => i % 2 == 0)),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<bool>, offset, (i, elem) => Assert.Equal(i % 2 == 0, elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new PrimitiveDataFrameColumn<bool>("nullable_bool", Enumerable.Range(0, numRows).Select(i => i % 10 == 0 ? null : (bool?) (i % 2 == 0))),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<bool?>, offset, (i, elem) => Assert.Equal(i % 10 == 0 ? null : i % 2 == 0, elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new StringDataFrameColumn("string", Enumerable.Range(0, numRows).Select(i => i.ToString())),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<string>, offset, (i, elem) => Assert.Equal(i.ToString(), elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new PrimitiveDataFrameColumn<DateTime>("dateTime", Enumerable.Range(0, numRows).Select(i => new DateTime(2021, 12, 8) + TimeSpan.FromSeconds(i))),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<DateTime>, offset, (i, elem) => Assert.Equal(new DateTime(2021, 12, 8) + TimeSpan.FromSeconds(i), elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new DecimalDataFrameColumn("decimal", Enumerable.Range(0, numRows).Select(i => new decimal(i) / 100)),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<decimal>, offset, (i, elem) => Assert.Equal(new decimal(i) / 100, elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new DecimalDataFrameColumn("nullable_decimal", Enumerable.Range(0, numRows).Select(i => i % 10 == 0 ? null : (decimal?) (new decimal(i) / 100))),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<decimal?>, offset, (i, elem) => Assert.Equal(i % 10 == 0 ? null : (new decimal(i) / 100), elem)),
                },
            };
        }

        private static void VerifyData<TElement>(
            LogicalColumnReader<TElement> columnReader, long offset, Action<long, TElement> verifier)
        {
            Assert.NotNull(columnReader);
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
