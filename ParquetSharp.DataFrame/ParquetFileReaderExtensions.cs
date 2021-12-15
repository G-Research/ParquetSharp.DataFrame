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
            return ToDataFrameImpl(fileReader, null, null);
        }

        /// <summary>
        /// Read specific row groups from a parquet file into a DataFrame
        /// </summary>
        /// <param name="fileReader">Reader for the ParquetFile to read</param>
        /// <param name="rowGroupIndices">Indices of row groups to read</param>
        /// <returns>Data from the Parquet file as a DataFrame</returns>
        public static DataFrame ToDataFrame(this ParquetFileReader fileReader, IReadOnlyList<int> rowGroupIndices)
        {
            return ToDataFrameImpl(fileReader, null, rowGroupIndices);
        }

        /// <summary>
        /// Read specific columns from a parquet file into a DataFrame
        /// </summary>
        /// <param name="fileReader">Reader for the ParquetFile to read</param>
        /// <param name="columns">List of columns to read</param>
        /// <returns>Column Data from the Parquet file as a DataFrame</returns>
        public static DataFrame ToDataFrame(this ParquetFileReader fileReader, IReadOnlyList<string> columns)
        {
            return ToDataFrameImpl(fileReader, columns, null);
        }

        /// <summary>
        /// Read specific columns and row groups from a parquet file into a DataFrame
        /// </summary>
        /// <param name="fileReader">Reader for the ParquetFile to read</param>
        /// <param name="columns">List of columns to read</param>
        /// <param name="rowGroupIndices">Indices of row groups to read</param>
        /// <returns>Column Data from the Parquet file as a DataFrame</returns>
        public static DataFrame ToDataFrame(
            this ParquetFileReader fileReader, IReadOnlyList<string> columns, IReadOnlyList<int> rowGroupIndices)
        {
            return ToDataFrameImpl(fileReader, columns, rowGroupIndices);
        }

        private static DataFrame ToDataFrameImpl(
            this ParquetFileReader fileReader, IReadOnlyList<string>? columns = null, IReadOnlyList<int>? rowGroupIndices = null)
        {
            var numColumns = columns?.Count ?? fileReader.FileMetaData.NumColumns;
            var dataFrameColumns = new List<DataFrameColumn>(numColumns);
            var numRows = rowGroupIndices == null ? fileReader.FileMetaData.NumRows : GetNumRows(fileReader, rowGroupIndices);

            var columnIndexMap = new int[numColumns];
            for (var i = 0; i < numColumns; ++i)
            {
                columnIndexMap[i] = columns == null ? i : FindColumnIndex(columns[i], fileReader.FileMetaData.Schema);
            }

            long offset = 0;
            var numRowGroups = rowGroupIndices?.Count ?? fileReader.FileMetaData.NumRowGroups;
            for (var rowGroupIdx = 0; rowGroupIdx < numRowGroups; ++rowGroupIdx)
            {
                var fileRowGroupIdx = rowGroupIndices == null ? rowGroupIdx : rowGroupIndices[rowGroupIdx];
                using var rowGroupReader = fileReader.RowGroup(fileRowGroupIdx);
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

        private static long GetNumRows(ParquetFileReader fileReader, IReadOnlyList<int> rowGroupIndices)
        {
            var totalRows = 0L;
            foreach (var rowGroupIdx in rowGroupIndices)
            {
                using var rowGroupReader = fileReader.RowGroup(rowGroupIdx);
                totalRows += rowGroupReader.MetaData.NumRows;
            }

            return totalRows;
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
