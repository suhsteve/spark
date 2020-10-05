// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Threading;
using Microsoft.Spark.Services;

namespace Microsoft.Spark.Network
{
    /// <summary>
    /// SocketFactory is used to create ISocketWrapper instance.
    /// </summary>
    internal static class SocketFactory
    {
        private static int s_socketsCreated = 0;
        private static readonly ILoggerService s_logger =
            LoggerServiceFactory.GetLogger(typeof(SocketFactory));

        /// <summary>
        /// Creates an ISocket instance based on the socket type set.
        /// </summary>
        /// <returns>
        /// ISocketWrapper instance.
        /// </returns>
        public static ISocketWrapper CreateSocket()
        {
            Interlocked.Increment(ref s_socketsCreated);
            s_logger.LogInfo($"CreateSocket() called {s_socketsCreated} times");
            var socketWrapper = new DefaultSocketWrapper();
            s_logger.LogInfo($"CreateSocket() ISocketWrapper hashcode [{socketWrapper.GetHashCode()}]");
            return socketWrapper;
        }
    }
}
