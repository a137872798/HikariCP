/*
 * Copyright (C) 2017 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zaxxer.hikari.metrics;

/**
 * 轨迹测量对象   测量相关的先不看吧 在 metrics下的3个子包 对应3种不同的 测量框架
 * @author Brett Wooldridge
 */
public interface IMetricsTracker extends AutoCloseable
{
   /**
    * 记录连接创建的时间
    * @param connectionCreatedMillis
    */
   default void recordConnectionCreatedMillis(long connectionCreatedMillis) {}

   /**
    * 记录获取连接的时间 以纳秒为单位
    * @param elapsedAcquiredNanos
    */
   default void recordConnectionAcquiredNanos(final long elapsedAcquiredNanos) {}

   /**
    * 记录使用连接的时间
    * @param elapsedBorrowedMillis
    */
   default void recordConnectionUsageMillis(final long elapsedBorrowedMillis) {}

   /**
    * 记录超时时间
    */
   default void recordConnectionTimeout() {}

   @Override
   default void close() {}
}
