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
            var columns = new Column[]
            {
                new Column<int>("int"),
                new Column<double>("double"),
                new Column<string>("string"),
                new Column<bool>("bool"),
                new Column<DateTime>("dateTime"),
                new Column<Decimal>("decimal", LogicalType.Decimal(29, 3)),
            };

            const int numRows = 10_000;
            var intData = Enumerable.Range(0, numRows).ToArray();
            var doubleData = Enumerable.Range(0, numRows).Select(i => (double) i).ToArray();
            var stringData = Enumerable.Range(0, numRows).Select(i => i.ToString()).ToArray();
            var boolData = Enumerable.Range(0, numRows).Select(i => i % 2 == 0).ToArray();
            var dateTimeData = Enumerable.Range(0, numRows)
                .Select(i => new DateTime(2021, 12, 8) + TimeSpan.FromSeconds(i)).ToArray();
            var decimalData = Enumerable.Range(0, numRows).Select(i => ((decimal) i) / 100).ToArray();

            using var buffer = new ResizableBuffer();
            using (var output = new BufferOutputStream(buffer))
            {
                var propertiesBuilder = new WriterPropertiesBuilder();
                using var fileWriter = new ParquetFileWriter(output, columns, propertiesBuilder.Build());
                using var rowGroupWriter = fileWriter.AppendRowGroup();

                using var intCol = rowGroupWriter.NextColumn().LogicalWriter<int>();
                intCol.WriteBatch(intData);

                using var doubleCol = rowGroupWriter.NextColumn().LogicalWriter<double>();
                doubleCol.WriteBatch(doubleData);

                using var stringCol = rowGroupWriter.NextColumn().LogicalWriter<string>();
                stringCol.WriteBatch(stringData);

                using var boolCol = rowGroupWriter.NextColumn().LogicalWriter<bool>();
                boolCol.WriteBatch(boolData);

                using var dateTimeCol = rowGroupWriter.NextColumn().LogicalWriter<DateTime>();
                dateTimeCol.WriteBatch(dateTimeData);

                using var decimalCol = rowGroupWriter.NextColumn().LogicalWriter<Decimal>();
                decimalCol.WriteBatch(decimalData);

                fileWriter.Close();
            }

            using (var input = new BufferReader(buffer))
            {
                using var fileReader = new ParquetFileReader(input);
                var dataFrame = fileReader.ToDataFrame();

                Assert.Equal(columns.Length, dataFrame.Columns.Count);

                var intCol = dataFrame["int"];
                Assert.IsType<PrimitiveDataFrameColumn<int>>(intCol);
                Assert.Equal(numRows, intCol.Length);
                Assert.Equal(0, intCol.NullCount);
                for (int i = 0; i < numRows; ++i)
                {
                    Assert.Equal(intCol[i], intData[i]);
                }

                var doubleCol = dataFrame["double"];
                Assert.IsType<PrimitiveDataFrameColumn<double>>(doubleCol);
                Assert.Equal(numRows, doubleCol.Length);
                Assert.Equal(0, doubleCol.NullCount);
                for (int i = 0; i < numRows; ++i)
                {
                    Assert.Equal(doubleData[i], doubleCol[i]);
                }

                var stringCol = dataFrame["string"];
                Assert.IsType<StringDataFrameColumn>(stringCol);
                Assert.Equal(numRows, stringCol.Length);
                Assert.Equal(0, stringCol.NullCount);
                for (int i = 0; i < numRows; ++i)
                {
                    Assert.Equal(stringData[i], stringCol[i]);
                }

                var boolCol = dataFrame["bool"];
                Assert.IsType<BooleanDataFrameColumn>(boolCol);
                Assert.Equal(numRows, boolCol.Length);
                Assert.Equal(0, boolCol.NullCount);
                for (int i = 0; i < numRows; ++i)
                {
                    Assert.Equal(boolData[i], boolCol[i]);
                }

                var dateTimeCol = dataFrame["dateTime"];
                Assert.IsType<PrimitiveDataFrameColumn<DateTime>>(dateTimeCol);
                Assert.Equal(numRows, dateTimeCol.Length);
                Assert.Equal(0, dateTimeCol.NullCount);
                for (int i = 0; i < numRows; ++i)
                {
                    Assert.Equal(dateTimeData[i], dateTimeCol[i]);
                }

                var decimalCol = dataFrame["decimal"];
                Assert.IsType<DecimalDataFrameColumn>(decimalCol);
                Assert.Equal(numRows, decimalCol.Length);
                Assert.Equal(0, decimalCol.NullCount);
                for (int i = 0; i < numRows; ++i)
                {
                    Assert.Equal(decimalData[i], decimalCol[i]);
                }

                fileReader.Close();
            }
        }
    }
}
