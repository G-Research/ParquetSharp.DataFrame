using Microsoft.Data.Analysis;

namespace ParquetSharp
{
    public class DataFrameWriter : ILogicalColumnWriterVisitor<bool>
    {
        public DataFrameWriter(DataFrameColumn dataFrameColumn, long offset, int batchSize)
        {
            _dataFrameColumn = dataFrameColumn;
            _offset = offset;
            _batchSize = batchSize;
        }

        public bool OnLogicalColumnWriter<TValue>(LogicalColumnWriter<TValue> columnWriter)
        {
            var values = new TValue[_batchSize];
            for (var i = 0; i < _batchSize; ++i)
            {
                values[i] = (TValue) _dataFrameColumn[_offset + i];
            }
            columnWriter.WriteBatch(values);
            return true;
        }

        private readonly DataFrameColumn _dataFrameColumn;
        private readonly long _offset;
        private readonly int _batchSize;
    }
}
