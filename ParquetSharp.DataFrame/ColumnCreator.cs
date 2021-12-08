using System;
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
            switch (columnReader)
            {
                case LogicalColumnReader<string>:
                    return new StringDataFrameColumn(_columnName, _numRows);
                case LogicalColumnReader<bool>:
                case LogicalColumnReader<bool?>:
                    return new BooleanDataFrameColumn(_columnName, _numRows);
                case LogicalColumnReader<int>:
                case LogicalColumnReader<int?>:
                    return new PrimitiveDataFrameColumn<int>(_columnName, _numRows);
                case LogicalColumnReader<long>:
                case LogicalColumnReader<long?>:
                    return new PrimitiveDataFrameColumn<long>(_columnName, _numRows);
                case LogicalColumnReader<float>:
                case LogicalColumnReader<float?>:
                    return new PrimitiveDataFrameColumn<float>(_columnName, _numRows);
                case LogicalColumnReader<double>:
                case LogicalColumnReader<double?>:
                    return new PrimitiveDataFrameColumn<double>(_columnName, _numRows);
                case LogicalColumnReader<DateTime>:
                case LogicalColumnReader<DateTime?>:
                    return new PrimitiveDataFrameColumn<DateTime>(_columnName, _numRows);
                case LogicalColumnReader<decimal>:
                case LogicalColumnReader<decimal?>:
                    return new DecimalDataFrameColumn(_columnName, _numRows);
                default:
                    throw new NotImplementedException($"Unsupported column logical type: {typeof(TElement)}");
            }
        }

        private readonly string _columnName;
        private readonly long _numRows;
    }
}
