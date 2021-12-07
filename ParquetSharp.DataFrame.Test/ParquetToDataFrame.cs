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
            };

            const int numRows = 1000;

            using var buffer = new ResizableBuffer();
            using (var output = new BufferOutputStream(buffer))
            {
                var propertiesBuilder = new WriterPropertiesBuilder();
                using var fileWriter = new ParquetFileWriter(output, columns, propertiesBuilder.Build());
                using var rowGroupWriter = fileWriter.AppendRowGroup();

                using var intCol = rowGroupWriter.NextColumn().LogicalWriter<int>();
                intCol.WriteBatch(Enumerable.Range(0, numRows).ToArray());

                using var doubleCol = rowGroupWriter.NextColumn().LogicalWriter<double>();
                doubleCol.WriteBatch(Enumerable.Range(0, numRows).Select(i => (double) i).ToArray());

                using var stringCol = rowGroupWriter.NextColumn().LogicalWriter<string>();
                stringCol.WriteBatch(Enumerable.Range(0, numRows).Select(i => i.ToString()).ToArray());

                using var boolCol = rowGroupWriter.NextColumn().LogicalWriter<bool>();
                boolCol.WriteBatch(Enumerable.Range(0, numRows).Select(i => i % 2 == 0).ToArray());

                using var dateTimeCol = rowGroupWriter.NextColumn().LogicalWriter<DateTime>();
                dateTimeCol.WriteBatch(
                    Enumerable.Range(0, numRows)
                        .Select(i => new DateTime(2021, 12, 8) + TimeSpan.FromSeconds(i)).ToArray());

                fileWriter.Close();
            }

            using (var input = new BufferReader(buffer))
            {
                using var fileReader = new ParquetFileReader(input);
                var dataFrame = fileReader.ToDataFrame();

                Assert.Equal(columns.Length, dataFrame.Columns.Count);

                var intCol = dataFrame["int"];
                Assert.IsType<PrimitiveDataFrameColumn<int>>(intCol);

                var doubleCol = dataFrame["double"];
                Assert.IsType<PrimitiveDataFrameColumn<double>>(doubleCol);

                var stringCol = dataFrame["string"];
                Assert.IsType<StringDataFrameColumn>(stringCol);

                var boolCol = dataFrame["bool"];
                Assert.IsType<BooleanDataFrameColumn>(boolCol);

                var dateTimeCol = dataFrame["dateTime"];
                Assert.IsType<PrimitiveDataFrameColumn<DateTime>>(dateTimeCol);

                fileReader.Close();
            }
        }
    }
}
