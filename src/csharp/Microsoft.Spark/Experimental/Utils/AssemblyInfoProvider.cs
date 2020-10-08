﻿// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Linq;
using System.Net;
using System.Reflection;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace Microsoft.Spark.Experimental.Utils
{
    /// <summary>
    /// Gets the <see cref="AssemblyInfo"/> for the "Microsoft.Spark" and "Microsoft.Spark.Worker"
    /// assemblies if they exist within the current execution context of this application domain.
    /// </summary>
    internal static class AssemblyInfoProvider
    {
        private const string MicrosoftSparkAssemblyName = "Microsoft.Spark";
        private const string MicrosoftSparkWorkerAssemblyName = "Microsoft.Spark.Worker";

        private static readonly Lazy<AssemblyInfo> s_microsoftSparkAssemblyInfo =
            new Lazy<AssemblyInfo>(() => CreateAssemblyInfo(MicrosoftSparkAssemblyName));

        private static readonly Lazy<AssemblyInfo> s_microsoftSparkWorkerAssemblyInfo =
            new Lazy<AssemblyInfo>(() => CreateAssemblyInfo(MicrosoftSparkWorkerAssemblyName));

        private static readonly Lazy<StructType> s_schema =
            new Lazy<StructType>(() => new StructType(
                new StructField[]
                {
                    new StructField("AssemblyName", new StringType(), isNullable: false),
                    new StructField("AssemblyVersion", new StringType(), isNullable: false),
                    new StructField("HostName", new StringType(), isNullable: false)
                }));

        internal static AssemblyInfo MicrosoftSparkAssemblyInfo() => s_microsoftSparkAssemblyInfo.Value;

        internal static AssemblyInfo MicrosoftSparkWorkerAssemblyInfo() =>
            s_microsoftSparkWorkerAssemblyInfo.Value;

        internal static StructType Schema() => s_schema.Value;

        internal static GenericRow ToGenericRow(this AssemblyInfo assemblyInfo) =>
            new GenericRow(new object[]
            {
                assemblyInfo.AssemblyName,
                assemblyInfo.AssemblyVersion,
                assemblyInfo.HostName
            });

        private static AssemblyInfo CreateAssemblyInfo(string assemblyName)
        {
            Assembly assembly = AppDomain
                .CurrentDomain
                .GetAssemblies()
                .Single(asm => asm.GetName().Name == assemblyName);

            AssemblyName asmName = assembly.GetName();
            return new AssemblyInfo
            {
                AssemblyName = asmName.Name,
                AssemblyVersion = asmName.Version.ToString(),
                HostName = Dns.GetHostName()
            };
        }

        internal class AssemblyInfo
        {
            internal string AssemblyName { get; set; }
            internal string AssemblyVersion { get; set; }
            internal string HostName { get; set; }
        }
    }
}
