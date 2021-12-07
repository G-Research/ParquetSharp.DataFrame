using System;
using System.Collections.Generic;
using Microsoft.Data.Analysis;

namespace ParquetSharp.DataFrame
{
    public static class ParquetFileReaderExtensions
    {
        public static Microsoft.Data.Analysis.DataFrame ToDataFrame(this ParquetFileReader fileReader)
        {
            var numColumns = fileReader.FileMetaData.NumColumns;
            var numRows = fileReader.FileMetaData.NumRows;
            var dataFrameColumns = new List<DataFrameColumn>(numColumns);

            for (int colIdx = 0; colIdx < numColumns; ++colIdx)
            {
                var descriptor = fileReader.FileMetaData.Schema.Column(colIdx);
                dataFrameColumns.Add(CreateColumn(descriptor, numRows));
            }

            long offset = 0;
            for (int rowGroupIdx = 0; rowGroupIdx < fileReader.FileMetaData.NumRowGroups; ++rowGroupIdx)
            {
                using var rowGroupReader = fileReader.RowGroup(rowGroupIdx);
                for (int colIdx = 0; colIdx < numColumns; ++colIdx)
                {
                    using var columnReader = rowGroupReader.Column(colIdx);
                    using var logicalReader = columnReader.LogicalReader();
                    logicalReader.Apply(new DataFrameSetter(dataFrameColumns[colIdx], offset));
                }
                offset += rowGroupReader.MetaData.NumRows;
            }

            return new Microsoft.Data.Analysis.DataFrame(dataFrameColumns);
        }

        private static DataFrameColumn CreateColumn(ColumnDescriptor descriptor, long numRows)
        {
            switch (descriptor.LogicalType)
            {
                case NoneLogicalType:
                case IntLogicalType:
                {
                    switch (descriptor.PhysicalType)
                    {
                        case PhysicalType.Int32:
                            return new PrimitiveDataFrameColumn<int>(descriptor.Name, numRows);
                        case PhysicalType.Int64:
                            return new PrimitiveDataFrameColumn<long>(descriptor.Name, numRows);
                        case PhysicalType.Float:
                            return new PrimitiveDataFrameColumn<float>(descriptor.Name, numRows);
                        case PhysicalType.Double:
                            return new PrimitiveDataFrameColumn<double>(descriptor.Name, numRows);
                        case PhysicalType.Boolean:
                            return new BooleanDataFrameColumn(descriptor.Name, numRows);
                        default:
                            throw new NotImplementedException($"Unsupported physical type: {descriptor.PhysicalType}");
                    }
                }
                case StringLogicalType:
                {
                    return new StringDataFrameColumn(descriptor.Name, numRows);
                }
                case TimestampLogicalType:
                {
                    return new PrimitiveDataFrameColumn<DateTime>(descriptor.Name, numRows);
                }
                default:
                {
                    throw new NotImplementedException($"Unsupported column logical type: {descriptor.LogicalType.Type}");
                }
            }
        }
    }
}
