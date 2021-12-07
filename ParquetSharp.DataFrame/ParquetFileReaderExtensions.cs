namespace ParquetSharp.DataFrame
{
    public static class ParquetFileReaderExtensions
    {
        public static Microsoft.Data.Analysis.DataFrame ToDataFrame(this ParquetFileReader fileReader)
        {
            return new Microsoft.Data.Analysis.DataFrame();
        }
    }
}
