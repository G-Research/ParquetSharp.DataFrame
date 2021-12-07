using System;
using Microsoft.Data.Analysis;

namespace ParquetSharp.DataFrame
{
    /// <summary>
    /// LogicalColumnReaderVisitor that sets values in a DataFrameColumn
    /// </summary>
    internal sealed class DataFrameSetter : ILogicalColumnReaderVisitor<bool>
    {
        public DataFrameSetter(DataFrameColumn dataFrameColumn, long offset)
        {
            _dataFrameColumn = dataFrameColumn;
            _offset = offset;
        }

        public bool OnLogicalColumnReader<TElement>(LogicalColumnReader<TElement> columnReader)
        {
            var buffer = new TElement[columnReader.BufferLength];
            long offset = 0;
            while (columnReader.HasNext)
            {
                var read = columnReader.ReadBatch((Span<TElement>) buffer);
                for (var i = 0; i != read; ++i)
                {
                    _dataFrameColumn[_offset + offset + i] = buffer[i];
                }
                offset += read;
            }
            return true;
        }

        private readonly DataFrameColumn _dataFrameColumn;
        private readonly long _offset;
    }
}
