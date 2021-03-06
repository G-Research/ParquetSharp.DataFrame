using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Analysis;

namespace ParquetSharp
{
    public static class DataFrameExtensions
    {
        /// <summary>
        /// Write a DataFrame in Parquet format
        /// </summary>
        /// <param name="dataFrame">The DataFrame to write</param>
        /// <param name="path">Path to write to</param>
        /// <param name="writerProperties">Optional writer properties that override the default properties</param>
        /// <param name="logicalTypeOverrides">Mapping from column names to Parquet logical types,
        /// overriding the default logical types. When writing decimal columns, a logical type must be provided
        /// to specify the precision and scale to use.</param>
        /// <param name="rowGroupSize">Maximum number of rows per row group</param>
        public static void ToParquet(
            this DataFrame dataFrame, string path, WriterProperties? writerProperties = null,
            IReadOnlyDictionary<string, LogicalType>? logicalTypeOverrides = null, int rowGroupSize = 1024 * 1024)
        {
            var schemaColumns = dataFrame.Columns.Select(col => GetSchemaColumn(
                col, logicalTypeOverrides != null && logicalTypeOverrides.TryGetValue(col.Name, out var logicalType) ? logicalType : null)).ToArray();
            using var fileWriter = writerProperties == null
                ? new ParquetFileWriter(path, schemaColumns)
                : new ParquetFileWriter(path, schemaColumns, writerProperties);

            var numRows = dataFrame.Rows.Count;
            var offset = 0L;

            while (offset < numRows)
            {
                var batchSize = (int)Math.Min(numRows - offset, rowGroupSize);
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                foreach (var dataFrameColumn in dataFrame.Columns)
                {
                    using var columnWriter = rowGroupWriter.NextColumn();
                    using var logicalWriter = columnWriter.LogicalWriter();
                    logicalWriter.Apply(new DataFrameWriter(dataFrameColumn, offset, batchSize));
                }

                offset += batchSize;
            }

            fileWriter.Close();
        }

        private static Column GetSchemaColumn(DataFrameColumn column, LogicalType? logicalTypeOverride)
        {
            var dataType = column.DataType;
            var nullable = column.NullCount > 0;
            if (dataType == typeof(decimal) && logicalTypeOverride == null)
            {
                throw new ArgumentException($"Logical type override must be specified for decimal column '{column.Name}'");
            }

            if (nullable && dataType.IsValueType)
            {
                dataType = typeof(Nullable<>).MakeGenericType(dataType);
            }

            return new Column(dataType, column.Name, logicalTypeOverride);
        }
    }
}
