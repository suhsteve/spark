// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Collections.Generic;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using System.Linq;
using System.Threading;
using Microsoft.Spark.Sql.Streaming;
using static Microsoft.Spark.Sql.Functions;
using System.Threading.Tasks;

namespace Microsoft.Spark.Examples.Sql.Batch
{
    /// <summary>
    /// A simple example demonstrating basic Spark SQL features.
    /// </summary>
    internal sealed class Basic : IExample
    {
        public void Run(string[] args)
        {
            SparkSession spark = SparkSession
                .Builder()
                .AppName("SQL Datasource example using .NET for Apache Spark")
                .Config("spark.some.config.option", "some-value")
                .GetOrCreate();

            for (int i = 0; i < 10000; ++i)
            {
                // Temporary folder to put our test stream input.
                using var srcTempDirectory = new TemporaryDirectory();
                // Temporary folder to write ForeachBatch output.
                using var dstTempDirectory = new TemporaryDirectory();

                Func<Column, Column> outerUdf = Udf<int, int>(i => i + 100);

                // id column: [0, 1, ..., 9]
                WriteCsv(0, 10, Path.Combine(srcTempDirectory.Path, "input1.csv"));

                DataStreamWriter dsw = spark
                    .ReadStream()
                    .Schema("id INT")
                    .Csv(srcTempDirectory.Path)
                    .WriteStream()
                    .ForeachBatch((df, id) =>
                    {
                        Console.WriteLine("ForeachBatch begin");
                        //Func<Column, Column> innerUdf = Udf<int, int>(i => i + 200);
                        //df.Select(outerUdf(innerUdf(Col("id"))))
                        //    .Write()
                        //    .Csv(Path.Combine(dstTempDirectory.Path, id.ToString()));
                        for (int i = 0; i < 1000; ++i)
                        {
                            df.IsEmpty();
                            GC.Collect();
                        }
                        //df.Write().Csv(Path.Combine(dstTempDirectory.Path, id.ToString()));
                        Console.WriteLine("ForeachBatch end");
                    });

                StreamingQuery sq = dsw.Start();

                // Process until all available data in the source has been processed and committed
                // to the ForeachBatch sink. 
                sq.ProcessAllAvailable();

                // Add new file to the source path. The spark stream will read any new files
                // added to the source path.
                // id column: [10, 11, ..., 19]
                WriteCsv(10, 10, Path.Combine(srcTempDirectory.Path, "input2.csv"));

                // Process until all available data in the source has been processed and committed
                // to the ForeachBatch sink.
                sq.ProcessAllAvailable();
                sq.Stop();

                // Verify folders in the destination path.
                //string[] csvPaths =
                //    Directory.GetDirectories(dstTempDirectory.Path).OrderBy(s => s).ToArray();
                //var expectedPaths = new string[]
                //{
                //Path.Combine(dstTempDirectory.Path, "0"),
                //Path.Combine(dstTempDirectory.Path, "1"),
                //};
                //System.Diagnostics.Debug.Assert(expectedPaths.SequenceEqual(csvPaths));

                // Read the generated csv paths and verify contents.
                //DataFrame df = spark
                //    .Read()
                //    .Schema("id INT")
                //    .Csv(csvPaths[0], csvPaths[1])
                //    .Sort("id");

                //IEnumerable<int> actualIds = df.Collect().Select(r => r.GetAs<int>("id"));
                //System.Diagnostics.Debug.Assert(Enumerable.Range(300, 20).SequenceEqual(actualIds));
            }


            //for (int j = 0; j < 10000; ++j)
            //{
            //    // Temporary folder to put our test stream input.
            //    using var srcTempDirectory = new TemporaryDirectory();
            //    // Temporary folder to write ForeachBatch output.
            //    using var dstTempDirectory = new TemporaryDirectory();

            //    Func<Column, Column> outerUdf = Udf<int, int>(i => i + 100);

            //    // id column: [0, 1, ..., 9]
            //    WriteCsv(0, 10, Path.Combine(srcTempDirectory.Path, "input1.csv"));

            //    DataStreamWriter dsw = spark
            //        .ReadStream()
            //        .Schema("id INT")
            //        .Csv(srcTempDirectory.Path)
            //        .WriteStream()
            //        .Format("console");

            //    StreamingQuery sq = dsw.Start();

            //    var tasks = new Task[2];
            //    tasks[0] = Task.Run(() => DoWork(spark));

            //    // Process until all available data in the source has been processed and committed
            //    // to the ForeachBatch sink. 
            //    tasks[1] = Task.Run(() => sq.ProcessAllAvailable());
            //    Task.WaitAll(tasks);

            //    // Add new file to the source path. The spark stream will read any new files
            //    // added to the source path.
            //    // id column: [10, 11, ..., 19]
            //    WriteCsv(10, 10, Path.Combine(srcTempDirectory.Path, "input2.csv"));


            //    // Process until all available data in the source has been processed and committed
            //    // to the ForeachBatch sink. 
            //    tasks[0] = Task.Run(() => sq.ProcessAllAvailable());
            //    tasks[1] = Task.Run(() => DoWork(spark));
            //    Task.WaitAll(tasks);
            //    sq.Stop();
            //}
        }

        private static void DoWork(SparkSession spark)
        {
            var data = new List<GenericRow>
                    {
                        new GenericRow(new object[] { "Alice", 20}),
                        new GenericRow(new object[] { "Bob", 30})
                    };
            var schema = new StructType(new List<StructField>()
                    {
                        new StructField("name", new StringType()),
                        new StructField("age", new IntegerType())
                    });
            DataFrame df = spark.CreateDataFrame(data, schema);

            Column column = df["name"];
            column = df["age"];

            df.ToDF();
            df.ToDF("name2", "age2");

            StructType s = df.Schema();

            df.PrintSchema();

            df.Explain();
            df.Explain(true);
            df.Explain(false);

            df.Show();
            df.Show(10);
            df.Show(10, 10);
            df.Show(10, 10, true);

            DataFrame tempDF = df.ToJSON();

            tempDF = df.Join(df);
            tempDF = df.Join(df, "name");
            tempDF = df.Join(df, new[] { "name" });
            tempDF = df.Join(df, new[] { "name" }, "outer");
            tempDF = df.Join(df, df["age"] == df["age"]);
            tempDF = df.Join(df, df["age"] == df["age"], "outer");

            tempDF = df.CrossJoin(df);

            tempDF = df.SortWithinPartitions("age");
            tempDF = df.SortWithinPartitions("age", "name");
            tempDF = df.SortWithinPartitions();
            tempDF = df.SortWithinPartitions(df["age"]);
            tempDF = df.SortWithinPartitions(df["age"], df["name"]);

            tempDF = df.Sort("age");
            tempDF = df.Sort("age", "name");
            tempDF = df.Sort();
            tempDF = df.Sort(df["age"]);
            tempDF = df.Sort(df["age"], df["name"]);

            tempDF = df.OrderBy("age");
            tempDF = df.OrderBy("age", "name");
            tempDF = df.OrderBy();
            tempDF = df.OrderBy(df["age"]);
            tempDF = df.OrderBy(df["age"], df["name"]);

            tempDF = df.Hint("broadcast");
            tempDF = df.Hint("broadcast", new[] { "hello", "world" });

            column = df.Col("age");

            column = df.ColRegex("age");

            tempDF = df.As("alias");

            tempDF = df.Alias("alias");

            tempDF = df.Select("age");
            tempDF = df.Select("age", "name");
            tempDF = df.Select();
            tempDF = df.Select(df["age"]);
            tempDF = df.Select(df["age"], df["name"]);

            tempDF = df.SelectExpr();
            tempDF = df.SelectExpr("age * 2");
            tempDF = df.SelectExpr("age * 2", "abs(age)");

            tempDF = df.Filter(df["age"] > 21);
            tempDF = df.Filter("age > 21");

            tempDF = df.Where(df["age"] > 21);
            tempDF = df.Where("age > 21");
        }

        private void WriteCsv(int start, int count, string path)
        {
            using var streamWriter = new StreamWriter(path);
            foreach (int i in Enumerable.Range(start, count))
            {
                streamWriter.WriteLine(i);
            }
        }



        internal sealed class TemporaryDirectory : IDisposable
        {
            private bool _disposed = false;

            /// <summary>
            /// Path to temporary folder.
            /// </summary>
            public string Path { get; }

            public TemporaryDirectory()
            {
                Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), Guid.NewGuid().ToString());
                Cleanup();
                Directory.CreateDirectory(Path);
                Path = $"{Path}{System.IO.Path.DirectorySeparatorChar}";
            }

            public void Dispose()
            {
                Dispose(true);
                GC.SuppressFinalize(this);
            }

            private void Cleanup()
            {
                if (File.Exists(Path))
                {
                    File.Delete(Path);
                }
                else if (Directory.Exists(Path))
                {
                    Directory.Delete(Path, true);
                }
            }

            private void Dispose(bool disposing)
            {
                if (_disposed)
                {
                    return;
                }

                if (disposing)
                {
                    Cleanup();
                }

                _disposed = true;
            }
        }
    }
}
