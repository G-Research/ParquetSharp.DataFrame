using System.Collections.Generic;
using Microsoft.Data.Analysis;

namespace ParquetSharp
{
    public static class ParquetFileReaderExtensions
    {
        public static DataFrame ToDataFrame(this ParquetFileReader fileReader)
        {
            var numColumns = fileReader.FileMetaData.NumColumns;
            var numRows = fileReader.FileMetaData.NumRows;
            var dataFrameColumns = new List<DataFrameColumn>(numColumns);

            long offset = 0;
            for (var rowGroupIdx = 0; rowGroupIdx < fileReader.FileMetaData.NumRowGroups; ++rowGroupIdx)
            {
                using var rowGroupReader = fileReader.RowGroup(rowGroupIdx);
                for (var colIdx = 0; colIdx < numColumns; ++colIdx)
                {
                    using var columnReader = rowGroupReader.Column(colIdx);
                    using var logicalReader = columnReader.LogicalReader();

                    if (rowGroupIdx == 0)
                    {
                        // On first row group, create columns
                        var columnCreator = new ColumnCreator(logicalReader.ColumnDescriptor.Name, numRows);
                        dataFrameColumns.Add(logicalReader.Apply(columnCreator));
                    }

                    // Read column data
                    logicalReader.Apply(new DataFrameSetter(dataFrameColumns[colIdx], offset));
                }
                offset += rowGroupReader.MetaData.NumRows;
            }

            return new DataFrame(dataFrameColumns);
        }
    }
}
