using System;
using System.Linq;
using Microsoft.Data.Analysis;

namespace ParquetSharp
{
    public static class DataFrameExtensions
    {
        public static void ToParquet(this DataFrame dataFrame, string path)
        {
            var propertiesBuilder = new WriterPropertiesBuilder();
            using var writerProperties = propertiesBuilder.Build();
            var schemaColumns = dataFrame.Columns.Select(GetSchemaColumn).ToArray();
            using var fileWriter = new ParquetFileWriter(path, schemaColumns, writerProperties);

            const int rowGroupSize = 1024 * 1024;
            var numRows = dataFrame.Rows.Count;
            var offset = 0L;

            while (offset < numRows)
            {
                var batchSize = (int) Math.Min(numRows - offset, rowGroupSize);
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

        private static Column GetSchemaColumn(DataFrameColumn column)
        {
            var dataType = column.DataType;
            var nullable = column.NullCount > 0;
            LogicalType logicalTypeOverride = null;
            if (dataType == typeof(decimal))
            {
                // TODO: Work out how best to set precision and scale. May need to make this configurable?
                // Parquet stores decimal as int value * 10^(-scale), and precision is number of digits of unscaled value
                logicalTypeOverride = LogicalType.Decimal(29, 3);
            }
            if (nullable && dataType.IsValueType)
            {
                dataType = typeof(Nullable<>).MakeGenericType(dataType);
            }
            return new Column(dataType, column.Name, logicalTypeOverride);
        }
    }
}
