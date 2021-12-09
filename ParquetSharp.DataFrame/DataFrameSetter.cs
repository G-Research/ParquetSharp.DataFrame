using System;
using Microsoft.Data.Analysis;

namespace ParquetSharp
{
    /// <summary>
    /// LogicalColumnReaderVisitor that sets values in a DataFrameColumn
    /// </summary>
    internal sealed class DataFrameSetter : ILogicalColumnReaderVisitor<bool>
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

        public bool OnLogicalColumnReader<TElement>(LogicalColumnReader<TElement> columnReader)
        {
            var buffer = new TElement[columnReader.BufferLength];
            long offset = 0;
            var converter = GetConverter<TElement>();
            while (columnReader.HasNext)
            {
                var read = columnReader.ReadBatch((Span<TElement>) buffer);
                if (converter == null)
                {
                    for (var i = 0; i != read; ++i)
                    {
                        _dataFrameColumn[_offset + offset + i] = buffer[i];
                    }
                }
                else
                {
                    for (var i = 0; i != read; ++i)
                    {
                        _dataFrameColumn[_offset + offset + i] = converter(buffer[i]);
                    }
                }
                offset += read;
            }
            return true;
        }

        private static Func<TElement, object?>? GetConverter<TElement>()
        {
            if (typeof(TElement) == typeof(Date))
            {
                return el => ((Date) (object) el!).Days;
            }
            if (typeof(TElement) == typeof(Date?))
            {
                return el => ((Date?) (object?) el)?.Days ?? null;
            }
            return null;
        }

        private readonly DataFrameColumn _dataFrameColumn;
        private readonly long _offset;
    }
}
