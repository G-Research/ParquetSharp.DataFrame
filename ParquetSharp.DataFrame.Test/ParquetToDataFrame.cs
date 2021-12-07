using System.Linq;
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
                new Column<bool>("bool")
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

                fileWriter.Close();
            }

            using (var input = new BufferReader(buffer))
            {
                using var fileReader = new ParquetFileReader(input);
                var dataFrame = fileReader.ToDataFrame();
                
                Assert.Equal(4, dataFrame.Columns.Count);
                    
                fileReader.Close();
            }
        }
    }
}
