using System;
using Microsoft.Data.Analysis;

namespace ParquetSharp
{
    /// <summary>
    /// LogicalColumnReaderVisitor that sets values in a DataFrameColumn
    /// </summary>
    internal sealed class DataFrameSetter : ILogicalColumnReaderVisitor<Unit>
    {
        /// <summary>
        /// Create a DataFrameSetter
        /// </summary>
        /// <param name="dataFrameColumn">The column to read data into</param>
        /// <param name="offset">Position to begin inserting data at</param>
        public DataFrameSetter(DataFrameColumn dataFrameColumn, long offset)
        {
            _dataFrameColumn = dataFrameColumn;
            _offset = offset;
        }

        public Unit OnLogicalColumnReader<TElement>(LogicalColumnReader<TElement> columnReader)
        {
            var buffer = new TElement[columnReader.BufferLength];
            long offset = 0;
            while (columnReader.HasNext)
            {
                var read = columnReader.ReadBatch((Span<TElement>)buffer);
                for (var i = 0; i != read; ++i)
                {
                    _dataFrameColumn[_offset + offset + i] = buffer[i];
                }

                offset += read;
            }

            return Unit.Instance;
        }

        private readonly DataFrameColumn _dataFrameColumn;
        private readonly long _offset;
    }
}
