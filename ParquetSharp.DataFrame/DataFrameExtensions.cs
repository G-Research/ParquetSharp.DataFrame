using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Analysis;

namespace ParquetSharp
{
    public static class DataFrameExtensions
    {
        /// <summary>
        /// Writes a Dataframe in Parquet format using existing writer.
        /// </summary>
        /// <param name="dataFrame">DataFrame to write.</param>
        /// <param name="fileWriter">Writer to use.</param>
        /// <param name="rowGroupSize">Maximum number of rows per row group</param>
        public static void ToParquet(this DataFrame dataFrame, ParquetFileWriter fileWriter, int rowGroupSize = 1024 * 1024)
        {
            long numRows = dataFrame.Rows.Count;
            long offset = 0L;

            while (offset < numRows)
            {
                int batchSize = (int)Math.Min(numRows - offset, rowGroupSize);
                using RowGroupWriter rowGroupWriter = fileWriter.AppendRowGroup();
                foreach (DataFrameColumn dataFrameColumn in dataFrame.Columns)
                {
                    using ColumnWriter columnWriter = rowGroupWriter.NextColumn();
                    using LogicalColumnWriter logicalWriter = columnWriter.LogicalWriter();
                    logicalWriter.Apply(new DataFrameWriter(dataFrameColumn, offset, batchSize));
                }

                offset += batchSize;
            }
        }

        /// <summary>
        /// Write DataFrames in Parquet format
        /// </summary>
        /// <param name="dataFrame">DataFrames to write</param>
        /// <param name="path">Path to write to</param>
        /// <param name="writerProperties">Optional writer properties that override the default properties</param>
        /// <param name="logicalTypeOverrides">Mapping from column names to Parquet logical types,
        /// overriding the default logical types. When writing decimal columns, a logical type must be provided
        /// to specify the precision and scale to use.</param>
        /// <param name="rowGroupSize">Maximum number of rows per row group</param>
        public static void ToParquet(
            this IEnumerable<DataFrame> dataFrames, string path, WriterProperties? writerProperties = null,
            IReadOnlyDictionary<string, LogicalType>? logicalTypeOverrides = null, int rowGroupSize = 1024 * 1024)
        {
            if (dataFrames is null)
                throw new ArgumentNullException(nameof(dataFrames));

            DataFrame firstDataFrame = dataFrames.First();
            using ParquetFileWriter fileWriter = GetParquetFileWriter(path, writerProperties, logicalTypeOverrides, firstDataFrame);

            foreach (DataFrame dataFrame in dataFrames)
            {
                dataFrame.ToParquet(fileWriter, rowGroupSize);
            }

            fileWriter.Close();
        }

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
        public static void ToParquet(this DataFrame dataFrame, string path, WriterProperties? writerProperties = null,
            IReadOnlyDictionary<string, LogicalType>? logicalTypeOverrides = null, int rowGroupSize = 1024 * 1024)
        {
            using ParquetFileWriter fileWriter = GetParquetFileWriter(path, writerProperties, logicalTypeOverrides, dataFrame);
            dataFrame.ToParquet(fileWriter, rowGroupSize);

            fileWriter.Close();
        }

        private static ParquetFileWriter GetParquetFileWriter(
            string path, WriterProperties writerProperties,
            IReadOnlyDictionary<string, LogicalType> logicalTypeOverrides, DataFrame firstDataFrame)
        {
            Column[] schemaColumns = firstDataFrame.Columns.Select(col => GetSchemaColumn(
                col,
                (logicalTypeOverrides != null && logicalTypeOverrides.TryGetValue(col.Name, out LogicalType logicalType))
                ? logicalType : null)).ToArray();

            return writerProperties == null
                            ? new ParquetFileWriter(path, schemaColumns)
                            : new ParquetFileWriter(path, schemaColumns, writerProperties);
        }

        private static Column GetSchemaColumn(DataFrameColumn column, LogicalType? logicalTypeOverride)
        {
            Type dataType = column.DataType;
            if (dataType == typeof(decimal) && logicalTypeOverride == null)
                throw new ArgumentException($"Logical type override must be specified for decimal column '{column.Name}'");

            if (dataType.IsValueType && Nullable.GetUnderlyingType(dataType) != null)
                dataType = typeof(Nullable<>).MakeGenericType(dataType);

            return new Column(dataType, column.Name, logicalTypeOverride);
        }
    }
}
