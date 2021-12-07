using System;
using System.Linq;
using Microsoft.Data.Analysis;
using ParquetSharp.IO;
using Xunit;

namespace ParquetSharp.DataFrame.Test
{
    public class ParquetToDataFrame
    {
        [Fact]
        public void TestToDataFrame()
        {
            var testColumns = GetTestColumns();
            const int numRows = 10_000;

            using var buffer = new ResizableBuffer();
            using (var output = new BufferOutputStream(buffer))
            {
                var propertiesBuilder = new WriterPropertiesBuilder();
                var columns = testColumns.Select(c => c.ParquetColumn).ToArray();
                using var fileWriter = new ParquetFileWriter(output, columns, propertiesBuilder.Build());
                using var rowGroupWriter = fileWriter.AppendRowGroup();

                foreach (var column in testColumns)
                {
                    column.WriteColumn(numRows, rowGroupWriter.NextColumn());
                }

                fileWriter.Close();
            }

            using (var input = new BufferReader(buffer))
            {
                using var fileReader = new ParquetFileReader(input);
                var dataFrame = fileReader.ToDataFrame();

                Assert.Equal(testColumns.Length, dataFrame.Columns.Count);

                foreach (var column in testColumns)
                {
                    var dataFrameColumn = dataFrame[column.ParquetColumn.Name];
                    Assert.IsType(column.ExpectedColumnType, dataFrameColumn);
                    Assert.Equal(numRows, dataFrameColumn.Length);
                    Assert.Equal(0, dataFrameColumn.NullCount);
                    column.VerifyColumn(dataFrameColumn);
                }

                fileReader.Close();
            }
        }

        private readonly struct TestColumn
        {
            public Column ParquetColumn { get; init; }
            public Type ExpectedColumnType { get; init;  }
            public Action<int, ColumnWriter> WriteColumn { get; init;  }
            public Action<DataFrameColumn> VerifyColumn { get; init;  }
        }

        private static TestColumn[] GetTestColumns()
        {
            return new []
            {
                new TestColumn
                {
                    ParquetColumn = new Column<int>("int"),
                    ExpectedColumnType = typeof(PrimitiveDataFrameColumn<int>),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<int>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(i, column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<double>("double"),
                    ExpectedColumnType = typeof(PrimitiveDataFrameColumn<double>),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<double>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => (double) i).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal((double) i, column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<string>("string"),
                    ExpectedColumnType = typeof(StringDataFrameColumn),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<string>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => i.ToString()).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(i.ToString(), column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<bool>("bool"),
                    ExpectedColumnType = typeof(BooleanDataFrameColumn),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<bool>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => i % 2 == 0).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(i % 2 == 0, column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<DateTime>("dateTime"),
                    ExpectedColumnType = typeof(PrimitiveDataFrameColumn<DateTime>),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<DateTime>();
                        logicalWriter.WriteBatch(
                            Enumerable.Range(0, numRows)
                                .Select(i => new DateTime(2021, 12, 8) + TimeSpan.FromSeconds(i)).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(new DateTime(2021, 12, 8) + TimeSpan.FromSeconds(i), column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<Decimal>("decimal", LogicalType.Decimal(29, 3)),
                    ExpectedColumnType = typeof(DecimalDataFrameColumn),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<Decimal>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows)
                                .Select(i => new Decimal(i) / 100).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(new Decimal(i) / 100, column[i]);
                        }
                    }
                },
            };
        }

    }
}
