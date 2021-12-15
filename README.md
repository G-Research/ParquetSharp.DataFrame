# ParquetSharp.DataFrame

ParquetSharp.DataFrame is a .NET library for reading and writing Apache Parquet files into/from .NET [DataFrames][1], using [ParquetSharp][2].

[1]: https://docs.microsoft.com/en-us/dotnet/api/microsoft.data.analysis.dataframe
[2]: https://github.com/G-Research/ParquetSharp

## Reading Parquet files

Parquet data is read into a `DataFrame` using `ToDataFrame` extension methods on `ParquetFileReader`,
for example:

```C#
using ParquetSharp;

using (var parquetReader = new ParquetFileReader(parquet_file_path))
{
    var dataFrame = parquetReader.ToDataFrame();
    parquetReader.Close();
}
```

Overloads are provided that allow you to read specific columns from the Parquet file,
and/or a subset of row groups:

```C#
var dataFrame = parquetReader.ToDataFrame(columns: new [] {"col_1", "col_2"});
```

```C#
var dataFrame = parquetReader.ToDataFrame(rowGroupIndices: new [] {0, 1});
```

## Writing Parquet files

Parquet files are written using the `ToParquet` extension method on `DataFrame`:

```C#
using ParquetSharp;
using Microsoft.Data.Analysis;

var dataFrame = new DataFrame(columns);
dataFrame.ToParquet(parquet_file_path);
```

Parquet writing options can be overridden by providing an instance of `WriterProperties`:

```C#
using (var propertiesBuilder = new WriterPropertiesBuilder())
{
    propertiesBuilder.Compression(Compression.Snappy);
    using (var properties = propertiesBuilder.Build())
    {
        dataFrame.ToParquet(parquet_file_path, properties);
    }
}
```

The logical type to use when writing a column can optionally be overridden.
This is required when writing decimal columns, as you must specify the precision and scale to be used
(see the [Parquet documentation](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal) for more details).
This also allows writing an integer column as a Parquet date or time.

```C#
dataFrame.ToParquet(parquet_file_path, logicalTypeOverrides: new Dictionary<string, LogicalType>
{
    {"decimal_column", LogicalType.Decimal(precision: 29, scale: 3)},
    {"date_column", LogicalType.Date()},
    {"time_column", LogicalType.Time(isAdjustedToUtc: true, TimeUnit.Millis)},
});
```
