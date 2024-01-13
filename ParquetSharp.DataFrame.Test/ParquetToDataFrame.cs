using System;
using System.Linq;
using Microsoft.Data.Analysis;
using ParquetSharp.IO;
using Xunit;

namespace ParquetSharp.DataFrame.Test
{
    /// <summary>
    /// Test reading Parquet data into a DataFrame
    /// </summary>
    public class ParquetToDataFrame
    {
        [Fact]
        public void TestToDataFrame()
        {
            var testColumns = GetTestColumns();
            const int numRows = 10_000;

            using var buffer = new ResizableBuffer();
            using (var output = new BufferOutputStream(buffer))
            {
                var columns = testColumns.Select(c => c.ParquetColumn).ToArray();
                using var fileWriter = new ParquetFileWriter(output, columns);
                using var rowGroupWriter = fileWriter.AppendRowGroup();

                foreach (var column in testColumns)
                {
                    column.WriteColumn(numRows, rowGroupWriter.NextColumn());
                }

                fileWriter.Close();
            }

            using (var input = new BufferReader(buffer))
            {
                using var fileReader = new ParquetFileReader(input);
                var dataFrame = fileReader.ToDataFrame();

                Assert.Equal(testColumns.Length, dataFrame.Columns.Count);

                foreach (var column in testColumns)
                {
                    var dataFrameColumn = dataFrame[column.ParquetColumn.Name];
                    Assert.IsType(column.ExpectedColumnType, dataFrameColumn);
                    Assert.Equal(numRows, dataFrameColumn.Length);
                    Assert.Equal(column.NullCount?.Invoke(numRows) ?? 0, dataFrameColumn.NullCount);
                    column.VerifyColumn(dataFrameColumn);
                }

                fileReader.Close();
            }
        }

        [Fact]
        public void TestSpecifyingColumnsToRead()
        {
            var testColumns = GetTestColumns();
            const int numRows = 10_000;

            using var buffer = new ResizableBuffer();
            using (var output = new BufferOutputStream(buffer))
            {
                var columns = testColumns.Select(c => c.ParquetColumn).ToArray();
                using var fileWriter = new ParquetFileWriter(output, columns);
                using var rowGroupWriter = fileWriter.AppendRowGroup();

                foreach (var column in testColumns)
                {
                    column.WriteColumn(numRows, rowGroupWriter.NextColumn());
                }

                fileWriter.Close();
            }

            var columnNames = new[] { "int", "nullable_bool", "float" };

            using (var input = new BufferReader(buffer))
            {
                using var fileReader = new ParquetFileReader(input);

                var dataFrame = fileReader.ToDataFrame(columns: columnNames);

                Assert.Equal(columnNames.Length, dataFrame.Columns.Count);

                foreach (var column in testColumns.Where(c => columnNames.Contains(c.ParquetColumn.Name)))
                {
                    var dataFrameColumn = dataFrame[column.ParquetColumn.Name];
                    Assert.IsType(column.ExpectedColumnType, dataFrameColumn);
                    Assert.Equal(numRows, dataFrameColumn.Length);
                    Assert.Equal(column.NullCount?.Invoke(numRows) ?? 0, dataFrameColumn.NullCount);
                    column.VerifyColumn(dataFrameColumn);
                }

                fileReader.Close();
            }
        }

        [Fact]
        public void TestSpecifyingRowGroupsToRead()
        {
            const int rowsPerRowGroup = 1024;
            const int numRowGroups = 10;

            using var buffer = new ResizableBuffer();
            using (var output = new BufferOutputStream(buffer))
            {
                var columns = new Column[] { new Column<int>("col") };
                using var fileWriter = new ParquetFileWriter(output, columns);

                for (var rowGroupIdx = 0; rowGroupIdx < numRowGroups; ++rowGroupIdx)
                {
                    using var rowGroupWriter = fileWriter.AppendRowGroup();
                    using var logicalWriter = rowGroupWriter.NextColumn().LogicalWriter<int>();
                    logicalWriter.WriteBatch(Enumerable.Range(0, rowsPerRowGroup).Select(i => rowGroupIdx * rowsPerRowGroup + i).ToArray());
                }

                fileWriter.Close();
            }

            using (var input = new BufferReader(buffer))
            {
                using var fileReader = new ParquetFileReader(input);
                // Specify a subset of row groups out of order
                var rowGroupIndices = new[] { 1, 6, 3, 4 };
                var dataFrame = fileReader.ToDataFrame(rowGroupIndices: rowGroupIndices);
                fileReader.Close();

                var column = dataFrame["col"];
                Assert.Equal(rowsPerRowGroup * rowGroupIndices.Length, column.Length);
                for (var i = 0; i < column.Length; ++i)
                {
                    var rowGroupIndex = rowGroupIndices[i / rowsPerRowGroup];
                    var expectedVal = rowGroupIndex * rowsPerRowGroup + i % rowsPerRowGroup;
                    Assert.Equal(expectedVal, column[i]);
                }
            }
        }

        [Fact]
        public void ThrowsOnInvalidColumnName()
        {
            var testColumns = GetTestColumns();
            const int numRows = 10_000;

            using var buffer = new ResizableBuffer();
            using (var output = new BufferOutputStream(buffer))
            {
                var columns = testColumns.Select(c => c.ParquetColumn).ToArray();
                using var fileWriter = new ParquetFileWriter(output, columns);
                using var rowGroupWriter = fileWriter.AppendRowGroup();

                foreach (var column in testColumns)
                {
                    column.WriteColumn(numRows, rowGroupWriter.NextColumn());
                }

                fileWriter.Close();
            }

            var columnNames = new[] { "does_not_exist" };

            using (var input = new BufferReader(buffer))
            {
                using var fileReader = new ParquetFileReader(input);

                var exception = Assert.Throws<ArgumentException>(() => fileReader.ToDataFrame(columns: columnNames));
                Assert.Contains("does_not_exist", exception.Message);
            }
        }

        private struct TestColumn
        {
            public Column ParquetColumn { get; init; }
            public Type ExpectedColumnType { get; init; }
            public Action<int, ColumnWriter> WriteColumn { get; init; }
            public Action<DataFrameColumn> VerifyColumn { get; init; }
            public Func<long, long>? NullCount { get; init; }
        }

        private static TestColumn[] GetTestColumns()
        {
            return new[]
            {
                new TestColumn
                {
                    ParquetColumn = new Column<byte>("uint8"),
                    ExpectedColumnType = typeof(ByteDataFrameColumn),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<byte>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => (byte)(i % 256)).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal((byte)(i % 256), column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<sbyte>("int8"),
                    ExpectedColumnType = typeof(SByteDataFrameColumn),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<sbyte>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => (sbyte)(i % 256 - 128)).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal((sbyte)(i % 256 - 128), column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<ushort>("uint16"),
                    ExpectedColumnType = typeof(UInt16DataFrameColumn),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<ushort>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => (ushort)(i % ushort.MaxValue)).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal((ushort)(i % ushort.MaxValue), column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<short>("int16"),
                    ExpectedColumnType = typeof(Int16DataFrameColumn),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<short>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => (short)(i % short.MaxValue)).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal((short)(i % short.MaxValue), column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<int>("int"),
                    ExpectedColumnType = typeof(Int32DataFrameColumn),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<int>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(i, column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<int?>("nullable_int"),
                    ExpectedColumnType = typeof(Int32DataFrameColumn),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<int?>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => i % 10 == 0 ? (int?)null : i).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(i % 10 == 0 ? null : (int?)i, column[i]);
                        }
                    },
                    NullCount = numRows => numRows / 10,
                },
                new TestColumn
                {
                    ParquetColumn = new Column<long>("long"),
                    ExpectedColumnType = typeof(Int64DataFrameColumn),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<long>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => (long)i).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal((long)i, column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<long?>("nullable_long"),
                    ExpectedColumnType = typeof(Int64DataFrameColumn),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<long?>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => i % 10 == 0 ? (long?)null : i).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (long i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(i % 10 == 0 ? null : (long?)i, column[i]);
                        }
                    },
                    NullCount = numRows => numRows / 10,
                },
                new TestColumn
                {
                    ParquetColumn = new Column<float>("float"),
                    ExpectedColumnType = typeof(SingleDataFrameColumn),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<float>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => (float)i).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal((float)i, column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<float?>("nullable_float"),
                    ExpectedColumnType = typeof(SingleDataFrameColumn),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<float?>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => i % 10 == 0 ? (float?)null : i).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(i % 10 == 0 ? null : (float?)i, column[i]);
                        }
                    },
                    NullCount = numRows => numRows / 10,
                },
                new TestColumn
                {
                    ParquetColumn = new Column<double>("double"),
                    ExpectedColumnType = typeof(DoubleDataFrameColumn),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<double>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => (double)i).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal((double)i, column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<double?>("nullable_double"),
                    ExpectedColumnType = typeof(DoubleDataFrameColumn),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<double?>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => i % 10 == 0 ? (double?)null : i).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(i % 10 == 0 ? null : (double?)i, column[i]);
                        }
                    },
                    NullCount = numRows => numRows / 10,
                },
                new TestColumn
                {
                    ParquetColumn = new Column<string>("string"),
                    ExpectedColumnType = typeof(StringDataFrameColumn),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<string>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => i.ToString()).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(i.ToString(), column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<bool>("bool"),
                    ExpectedColumnType = typeof(BooleanDataFrameColumn),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<bool>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => i % 2 == 0).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(i % 2 == 0, column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<bool?>("nullable_bool"),
                    ExpectedColumnType = typeof(BooleanDataFrameColumn),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<bool?>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => i % 10 == 0 ? (bool?)null : (i % 2 == 0)).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(i % 10 == 0 ? null : (bool?)(i % 2 == 0), column[i]);
                        }
                    },
                    NullCount = numRows => numRows / 10,
                },
                new TestColumn
                {
                    ParquetColumn = new Column<DateTime>("dateTime"),
                    ExpectedColumnType = typeof(DateTimeDataFrameColumn),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<DateTime>();
                        logicalWriter.WriteBatch(
                            Enumerable.Range(0, numRows)
                                .Select(i => new DateTime(2021, 12, 8) + TimeSpan.FromSeconds(i)).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(new DateTime(2021, 12, 8) + TimeSpan.FromSeconds(i), column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<DateTime?>("nullable_dateTime"),
                    ExpectedColumnType = typeof(DateTimeDataFrameColumn),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<DateTime?>();
                        logicalWriter.WriteBatch(
                            Enumerable.Range(0, numRows)
                                .Select(i => i % 10 == 0 ? (DateTime?)null : new DateTime(2021, 12, 8) + TimeSpan.FromSeconds(i)).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(i % 10 == 0 ? null : (DateTime?)new DateTime(2021, 12, 8) + TimeSpan.FromSeconds(i), column[i]);
                        }
                    },
                    NullCount = numRows => numRows / 10,
                },
                new TestColumn
                {
                    ParquetColumn = new Column<DateTimeNanos>("dateTimeNanos"),
                    ExpectedColumnType = typeof(PrimitiveDataFrameColumn<DateTimeNanos>),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<DateTimeNanos>();
                        logicalWriter.WriteBatch(
                            Enumerable.Range(0, numRows)
                                .Select(i => new DateTimeNanos(i)).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(new DateTimeNanos(i), column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<DateTimeNanos?>("nullable_dateTimeNanos"),
                    ExpectedColumnType = typeof(PrimitiveDataFrameColumn<DateTimeNanos>),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<DateTimeNanos?>();
                        logicalWriter.WriteBatch(
                            Enumerable.Range(0, numRows)
                                .Select(i => (DateTimeNanos?)new DateTimeNanos(i)).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(new DateTimeNanos(i), column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<decimal>("decimal", LogicalType.Decimal(29, 3)),
                    ExpectedColumnType = typeof(DecimalDataFrameColumn),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<decimal>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows)
                            .Select(i => new decimal(i) / 100).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(new decimal(i) / 100, column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<decimal?>("nullable_decimal", LogicalType.Decimal(29, 3)),
                    ExpectedColumnType = typeof(DecimalDataFrameColumn),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<decimal?>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows)
                            .Select(i => i % 10 == 0 ? (decimal?)null : new decimal(i) / 100).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(i % 10 == 0 ? null : (decimal?)new decimal(i) / 100, column[i]);
                        }
                    },
                    NullCount = numRows => numRows / 10,
                },
                new TestColumn
                {
                    ParquetColumn = new Column<Date>("date"),
                    ExpectedColumnType = typeof(PrimitiveDataFrameColumn<Date>),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<Date>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => new Date(i)).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(new Date(i), column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<Date?>("nullable_date"),
                    ExpectedColumnType = typeof(PrimitiveDataFrameColumn<Date>),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<Date?>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => (Date?)new Date(i)).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(new Date(i), column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<TimeSpan>("time_ms", LogicalType.Time(isAdjustedToUtc: true, TimeUnit.Millis)),
                    ExpectedColumnType = typeof(PrimitiveDataFrameColumn<TimeSpan>),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<TimeSpan>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => TimeSpan.FromMilliseconds(i)).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(TimeSpan.FromMilliseconds(i), column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<TimeSpan>("time_us", LogicalType.Time(isAdjustedToUtc: true, TimeUnit.Micros)),
                    ExpectedColumnType = typeof(PrimitiveDataFrameColumn<TimeSpan>),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<TimeSpan>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => TimeSpan.FromMilliseconds(i)).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(TimeSpan.FromMilliseconds(i), column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<TimeSpan?>("nullable_time_us", LogicalType.Time(isAdjustedToUtc: true, TimeUnit.Micros)),
                    ExpectedColumnType = typeof(PrimitiveDataFrameColumn<TimeSpan>),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<TimeSpan?>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => (TimeSpan?)TimeSpan.FromMilliseconds(i)).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(TimeSpan.FromMilliseconds(i), column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<TimeSpanNanos>("time_ns", LogicalType.Time(isAdjustedToUtc: true, TimeUnit.Nanos)),
                    ExpectedColumnType = typeof(PrimitiveDataFrameColumn<TimeSpanNanos>),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<TimeSpanNanos>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => new TimeSpanNanos(i)).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(new TimeSpanNanos(i), column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<TimeSpanNanos?>("nullable_time_ns", LogicalType.Time(isAdjustedToUtc: true, TimeUnit.Nanos)),
                    ExpectedColumnType = typeof(PrimitiveDataFrameColumn<TimeSpanNanos>),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<TimeSpanNanos?>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => (TimeSpanNanos?)new TimeSpanNanos(i)).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(new TimeSpanNanos(i), column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<Guid>("guid", LogicalType.Uuid()),
                    ExpectedColumnType = typeof(PrimitiveDataFrameColumn<Guid>),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<Guid>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => new Guid(i, 0, 0, new byte[]{0,0,0,0,0,0,0,0})).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(new Guid(i, 0, 0, new byte[]{0,0,0,0,0,0,0,0}), column[i]);
                        }
                    }
                },
                new TestColumn
                {
                    ParquetColumn = new Column<Guid?>("nullable_guid", LogicalType.Uuid()),
                    ExpectedColumnType = typeof(PrimitiveDataFrameColumn<Guid>),
                    WriteColumn = (numRows, columnWriter) =>
                    {
                        using var logicalWriter = columnWriter.LogicalWriter<Guid?>();
                        logicalWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => (Guid?)new Guid(i, 0, 0, new byte[]{0,0,0,0,0,0,0,0})).ToArray());
                    },
                    VerifyColumn = column =>
                    {
                        for (int i = 0; i < column.Length; ++i)
                        {
                            Assert.Equal(new Guid(i, 0, 0, new byte[]{0, 0, 0, 0, 0, 0, 0, 0 }), column[i]);
                        }
                    }
                },
            };
        }
    }
}
