using System;
using System.Diagnostics;
using Microsoft.Data.Analysis;

namespace ParquetSharp
{
    /// <summary>
    /// LogicalColumnReaderVisitor that creates DataFrameColumns of the correct type,
    /// corresponding to the Parquet LogicalColumnReader type.
    /// </summary>
    internal sealed class ColumnCreator : ILogicalColumnReaderVisitor<DataFrameColumn>
    {
        /// <summary>
        /// Create a ColumnCreator
        /// </summary>
        /// <param name="columnName">Name of the column to create</param>
        /// <param name="numRows">Total number of rows in the column</param>
        public ColumnCreator(string columnName, long numRows)
        {
            _columnName = columnName;
            _numRows = numRows;
        }

        public DataFrameColumn OnLogicalColumnReader<TElement>(LogicalColumnReader<TElement> columnReader)
        {
            if (columnReader.ColumnDescriptor.LogicalType is IntLogicalType intType)
            {
                return ColumnFromIntType(intType);
            }

            switch (columnReader)
            {
                case LogicalColumnReader<string>:
                    return new StringDataFrameColumn(_columnName, _numRows);
                case LogicalColumnReader<bool>:
                case LogicalColumnReader<bool?>:
                    return new BooleanDataFrameColumn(_columnName, _numRows);
                case LogicalColumnReader<byte>:
                case LogicalColumnReader<byte?>:
                    return new ByteDataFrameColumn(_columnName, _numRows);
                case LogicalColumnReader<sbyte>:
                case LogicalColumnReader<sbyte?>:
                    return new SByteDataFrameColumn(_columnName, _numRows);
                case LogicalColumnReader<ushort>:
                case LogicalColumnReader<ushort?>:
                    return new UInt16DataFrameColumn(_columnName, _numRows);
                case LogicalColumnReader<short>:
                case LogicalColumnReader<short?>:
                    return new Int16DataFrameColumn(_columnName, _numRows);
                case LogicalColumnReader<uint>:
                case LogicalColumnReader<uint?>:
                    return new UInt32DataFrameColumn(_columnName, _numRows);
                case LogicalColumnReader<int>:
                case LogicalColumnReader<int?>:
                    return new Int32DataFrameColumn(_columnName, _numRows);
                case LogicalColumnReader<ulong>:
                case LogicalColumnReader<ulong?>:
                    return new UInt64DataFrameColumn(_columnName, _numRows);
                case LogicalColumnReader<long>:
                case LogicalColumnReader<long?>:
                    return new Int64DataFrameColumn(_columnName, _numRows);
                case LogicalColumnReader<float>:
                case LogicalColumnReader<float?>:
                    return new SingleDataFrameColumn(_columnName, _numRows);
                case LogicalColumnReader<double>:
                case LogicalColumnReader<double?>:
                    return new DoubleDataFrameColumn(_columnName, _numRows);
                case LogicalColumnReader<DateTime>:
                case LogicalColumnReader<DateTime?>:
                    return new DateTimeDataFrameColumn(_columnName, _numRows);
                case LogicalColumnReader<DateTimeNanos>:
                case LogicalColumnReader<DateTimeNanos?>:
                    return new PrimitiveDataFrameColumn<DateTimeNanos>(_columnName, _numRows);
                case LogicalColumnReader<decimal>:
                case LogicalColumnReader<decimal?>:
                    return new DecimalDataFrameColumn(_columnName, _numRows);
                case LogicalColumnReader<Date>:
                case LogicalColumnReader<Date?>:
                    return new PrimitiveDataFrameColumn<Date>(_columnName, _numRows);
                case LogicalColumnReader<TimeSpan>:
                case LogicalColumnReader<TimeSpan?>:
                    return new PrimitiveDataFrameColumn<TimeSpan>(_columnName, _numRows);
                case LogicalColumnReader<TimeSpanNanos>:
                case LogicalColumnReader<TimeSpanNanos?>:
                    return new PrimitiveDataFrameColumn<TimeSpanNanos>(_columnName, _numRows);
                default:
                    throw new NotImplementedException($"Unsupported column logical type: {typeof(TElement)}");
            }
        }

        private DataFrameColumn ColumnFromIntType(IntLogicalType intType)
        {
            return (intType.BitWidth, intType.IsSigned) switch
            {
                (8, false) => new ByteDataFrameColumn(_columnName, _numRows),
                (8, true) => new SByteDataFrameColumn(_columnName, _numRows),
                (16, false) => new UInt16DataFrameColumn(_columnName, _numRows),
                (16, true) => new Int16DataFrameColumn(_columnName, _numRows),
                (32, false) => new UInt32DataFrameColumn(_columnName, _numRows),
                (32, true) => new Int32DataFrameColumn(_columnName, _numRows),
                (_, false) => new UInt64DataFrameColumn(_columnName, _numRows),
                (_, true) => new Int64DataFrameColumn(_columnName, _numRows)
            };
        }

        private readonly string _columnName;
        private readonly long _numRows;
    }
}
