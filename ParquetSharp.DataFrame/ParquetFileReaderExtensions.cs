using System;
using System.Collections.Generic;
using Microsoft.Data.Analysis;

namespace ParquetSharp
{
    public static class ParquetFileReaderExtensions
    {
        /// <summary>
        /// Read all data from a parquet file into a DataFrame
        /// </summary>
        /// <param name="fileReader">Reader for the ParquetFile to read</param>
        /// <returns>Data from the Parquet file as a DataFrame</returns>
        public static DataFrame ToDataFrame(this ParquetFileReader fileReader)
        {
            return ToDataFrameImpl(fileReader, null);
        }

        /// <summary>
        /// Read specific columns from a parquet file into a DataFrame
        /// </summary>
        /// <param name="fileReader">Reader for the ParquetFile to read</param>
        /// <param name="columns">List of columns to read</param>
        /// <returns>Column Data from the Parquet file as a DataFrame</returns>
        public static DataFrame ToDataFrame(this ParquetFileReader fileReader, IReadOnlyList<string> columns)
        {
            return ToDataFrameImpl(fileReader, columns);
        }

        private static DataFrame ToDataFrameImpl(this ParquetFileReader fileReader, IReadOnlyList<string>? columns = null)
        {
            var numColumns = columns?.Count ?? fileReader.FileMetaData.NumColumns;
            var numRows = fileReader.FileMetaData.NumRows;
            var dataFrameColumns = new List<DataFrameColumn>(numColumns);

            var columnIndexMap = new int[numColumns];
            for (var i = 0; i < numColumns; ++i)
            {
                columnIndexMap[i] = columns == null ? i : FindColumnIndex(columns[i], fileReader.FileMetaData.Schema);
            }

            long offset = 0;
            for (var rowGroupIdx = 0; rowGroupIdx < fileReader.FileMetaData.NumRowGroups; ++rowGroupIdx)
            {
                using var rowGroupReader = fileReader.RowGroup(rowGroupIdx);
                for (var colIdx = 0; colIdx < numColumns; ++colIdx)
                {
                    using var columnReader = rowGroupReader.Column(columnIndexMap[colIdx]);
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

        private static int FindColumnIndex(string column, SchemaDescriptor schema)
        {
            var index = schema.ColumnIndex(column);
            if (index < 0)
            {
                throw new ArgumentException($"Invalid column path '{column}'");
            }
            return index;
        }
    }
}
