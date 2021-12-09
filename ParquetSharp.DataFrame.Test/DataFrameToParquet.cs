using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Data.Analysis;
using Xunit;

namespace ParquetSharp.DataFrame.Test
{
    /// <summary>
    /// Test writing DataFrames to Parquet
    /// </summary>
    public class DataFrameToParquet
    {
        [Fact]
        public void TestToParquet()
        {
            int numRows = 10_000;
            var testColumns = GetTestColumns();
            var columns = new List<DataFrameColumn>(testColumns.Length);
            var logicalTypeOverrides = new Dictionary<string, LogicalType>();
            foreach (var testCol in testColumns)
            {
                var dataFrameCol = testCol.GetColumn(numRows);
                if (testCol.LogicalTypeOverride != null)
                {
                    logicalTypeOverrides[dataFrameCol.Name] = testCol.LogicalTypeOverride;
                }
                columns.Add(dataFrameCol);
            }

            var dataFrame = new Microsoft.Data.Analysis.DataFrame(columns);

            using var dir = new UnitTestDisposableDirectory();
            var filePath = Path.Join(dir.Info.FullName, "test.parquet");
            dataFrame.ToParquet(filePath, logicalTypeOverrides: logicalTypeOverrides);

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

        [Fact]
        public void TestCustomParquetWriterProperties()
        {
            int numRows = 10_000;
            var testColumns = GetTestColumns().Where(c => c.GetColumn(1).Name == "int32").ToArray();
            Assert.Single(testColumns);

            using var dir = new UnitTestDisposableDirectory();
            var filePath = Path.Join(dir.Info.FullName, "test.parquet");

            {
                var columns = new[] {testColumns[0].GetColumn(numRows)};
                var dataFrame = new Microsoft.Data.Analysis.DataFrame(columns);

                using var propertiesBuilder = new WriterPropertiesBuilder();
                propertiesBuilder.Compression(Compression.Gzip);
                using var properties = propertiesBuilder.Build();

                dataFrame.ToParquet(filePath, properties);
            }

            Assert.True(File.Exists(filePath));

            using var fileReader = new ParquetFileReader(filePath);
            Assert.Equal(testColumns.Length, fileReader.FileMetaData.NumColumns);
            Assert.Equal(numRows, fileReader.FileMetaData.NumRows);

            for (var rowGroupIdx = 0; rowGroupIdx < fileReader.FileMetaData.NumRowGroups; ++rowGroupIdx)
            {
                using var rowGroupReader = fileReader.RowGroup(rowGroupIdx);
                var columnMetadata = rowGroupReader.MetaData.GetColumnChunkMetaData(0);
                Assert.Equal(Compression.Gzip, columnMetadata.Compression);
            }
        }

        private readonly struct TestColumn
        {
            public Func<int, DataFrameColumn> GetColumn { get; init; }

            public Action<LogicalColumnReader, long> VerifyData { get; init; }

            public LogicalType? LogicalTypeOverride { get; init; }
        }

        private static TestColumn[] GetTestColumns()
        {
            return new[]
            {
                new TestColumn
                {
                    GetColumn = numRows =>
                        new ByteDataFrameColumn("uint8", Enumerable.Range(0, numRows).Select(i => (byte) (i % 256))),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<byte>, offset, (i, elem) => Assert.Equal(i % 256, elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new SByteDataFrameColumn("int8", Enumerable.Range(0, numRows).Select(i => (sbyte) (i % 256 - 128))),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<sbyte>, offset, (i, elem) => Assert.Equal(i % 256 - 128, elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new UInt16DataFrameColumn("uint16", Enumerable.Range(0, numRows).Select(i => (ushort) (i % ushort.MaxValue))),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<ushort>, offset, (i, elem) => Assert.Equal(i % ushort.MaxValue, elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new Int16DataFrameColumn("int16", Enumerable.Range(0, numRows).Select(i => (short) (i % short.MaxValue))),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<short>, offset, (i, elem) => Assert.Equal(i % short.MaxValue, elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new UInt32DataFrameColumn("uint32", Enumerable.Range(0, numRows).Select(i => (uint) i)),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<uint>, offset, (i, elem) => Assert.Equal(i, elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new Int32DataFrameColumn("int32", Enumerable.Range(0, numRows).Select(i => i)),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<int>, offset, (i, elem) => Assert.Equal(i, elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new Int32DataFrameColumn("nullable_int32", Enumerable.Range(0, numRows).Select(i => i % 10 == 0 ? (int?) null : i)),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<int?>, offset, (i, elem) => Assert.Equal(i % 10 == 0 ? null : (int?) i, elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new Int64DataFrameColumn("int64", Enumerable.Range(0, numRows).Select(i => (long) i)),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<long>, offset, Assert.Equal),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new Int64DataFrameColumn("nullable_int64", Enumerable.Range(0, numRows).Select(i => i % 10 == 0 ? (long?) null : i)),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<long?>, offset, (i, elem) => Assert.Equal(i % 10 == 0 ? null : i, elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new SingleDataFrameColumn("float", Enumerable.Range(0, numRows).Select(i => (float) i)),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<float>, offset, (i, elem) => Assert.Equal(i, elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new SingleDataFrameColumn("nullable_float", Enumerable.Range(0, numRows).Select(i => i % 10 == 0 ? null : (float?) i)),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<float?>, offset, (i, elem) => Assert.Equal(i % 10 == 0 ? null : (float?) i, elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new DoubleDataFrameColumn("double", Enumerable.Range(0, numRows).Select(i => (double) i)),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<double>, offset, (i, elem) => Assert.Equal(i, elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new DoubleDataFrameColumn("nullable_double", Enumerable.Range(0, numRows).Select(i => i % 10 == 0 ? null : (double?) i)),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<double?>, offset, (i, elem) => Assert.Equal(i % 10 == 0 ? null : (double?) i, elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new BooleanDataFrameColumn("bool", Enumerable.Range(0, numRows).Select(i => i % 2 == 0)),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<bool>, offset, (i, elem) => Assert.Equal(i % 2 == 0, elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new BooleanDataFrameColumn("nullable_bool", Enumerable.Range(0, numRows).Select(i => i % 10 == 0 ? null : (bool?) (i % 2 == 0))),
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
                        new PrimitiveDataFrameColumn<DateTime>("nullable_dateTime", Enumerable.Range(0, numRows).Select(i => i % 10 == 0 ? null : (DateTime?) (new DateTime(2021, 12, 8) + TimeSpan.FromSeconds(i)))),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<DateTime?>, offset, (i, elem) => Assert.Equal(i % 10 == 0 ? null : new DateTime(2021, 12, 8) + TimeSpan.FromSeconds(i), elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new PrimitiveDataFrameColumn<TimeSpan>("timeSpan", Enumerable.Range(0, numRows).Select(i => TimeSpan.FromMilliseconds(i))),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<TimeSpan>, offset, (i, elem) => Assert.Equal(TimeSpan.FromMilliseconds(i), elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new PrimitiveDataFrameColumn<TimeSpan>("nullable_timeSpan", Enumerable.Range(0, numRows).Select(i => i % 10 == 0 ? null : (TimeSpan?) TimeSpan.FromMilliseconds(i))),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<TimeSpan?>, offset, (i, elem) => Assert.Equal(i % 10 == 0 ? null : TimeSpan.FromMilliseconds(i), elem)),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new DecimalDataFrameColumn("decimal", Enumerable.Range(0, numRows).Select(i => new decimal(i) / 100)),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<decimal>, offset, (i, elem) => Assert.Equal(new decimal(i) / 100, elem)),
                    LogicalTypeOverride = LogicalType.Decimal(29, 3),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new DecimalDataFrameColumn("nullable_decimal", Enumerable.Range(0, numRows).Select(i => i % 10 == 0 ? null : (decimal?) (new decimal(i) / 100))),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<decimal?>, offset, (i, elem) => Assert.Equal(i % 10 == 0 ? null : (new decimal(i) / 100), elem)),
                    LogicalTypeOverride = LogicalType.Decimal(29, 3),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new Int32DataFrameColumn("int_as_byte", Enumerable.Range(0, numRows).Select(i => i % 256)),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<byte>, offset, (i, elem) => Assert.Equal(i % 256, elem)),
                    LogicalTypeOverride = LogicalType.Int(8, false),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new Int32DataFrameColumn("int_as_date", Enumerable.Range(0, numRows).Select(i => i)),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<Date>, offset, (i, elem) => Assert.Equal(new Date(1970, 1, 1).AddDays((int) i), elem)),
                    LogicalTypeOverride = LogicalType.Date(),
                },
                new TestColumn
                {
                    GetColumn = numRows =>
                        new Int32DataFrameColumn("int_as_time", Enumerable.Range(0, numRows).Select(i => i)),
                    VerifyData = (reader, offset) =>
                        VerifyData(reader as LogicalColumnReader<TimeSpan>, offset, (i, elem) => Assert.Equal(TimeSpan.FromMilliseconds(i), elem)),
                    LogicalTypeOverride = LogicalType.Time(isAdjustedToUtc: true, TimeUnit.Millis),
                },
            };
        }

        private static void VerifyData<TElement>(
            LogicalColumnReader<TElement>? columnReader, long offset, Action<long, TElement> verifier)
        {
            Assert.NotNull(columnReader);
            var buffer = new TElement[columnReader!.BufferLength];
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
