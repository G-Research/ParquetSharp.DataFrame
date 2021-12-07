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
            var columns = new List<DataFrameColumn>(numColumns);

            for (int i = 0; i < numColumns; ++i)
            {
                var descriptor = fileReader.FileMetaData.Schema.Column(i);
                columns.Add(CreateColumn(descriptor, numRows));
            }

            return new Microsoft.Data.Analysis.DataFrame(columns);
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
