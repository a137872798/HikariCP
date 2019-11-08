/*
 * Copyright (C) 2015 Brett Wooldridge
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

import static com.zaxxer.hikari.util.ClockSource.currentTime;
import static com.zaxxer.hikari.util.ClockSource.plusMillis;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 连接池统计对象
 * @author Brett Wooldridge
 */
public abstract class PoolStats
{
   /**
    * 重新加载数据的时间戳 每次尝试获取数据 都会往后加
    */
   private final AtomicLong reloadAt;
   /**
    * reloadAt 加载的基准值
    */
   private final long timeoutMs;

   // protected 修饰是为了让子类可以访问到

   /**
    * 当前总的连接数
    */
   protected volatile int totalConnections;
   /**
    * 空闲连接数
    */
   protected volatile int idleConnections;
   /**
    * 活跃连接数
    */
   protected volatile int activeConnections;
   /**
    * 当前悬置的线程数
    */
   protected volatile int pendingThreads;
   /**
    * 最大连接数
    */
   protected volatile int maxConnections;
   /**
    * 最小连接数
    */
   protected volatile int minConnections;

   /**
    * 通过一个超时时间对象来初始化
    * @param timeoutMs
    */
   public PoolStats(final long timeoutMs)
   {
      this.timeoutMs = timeoutMs;
      // 默认情况该值为 0
      this.reloadAt = new AtomicLong();
   }

   // 执行对应的方法前 都会调用 shouldLoad

   /**
    * 获取最大连接数
    * @return
    */
   public int getTotalConnections()
   {
      // 如果本次时间到了 需要重新加载的时间
      if (shouldLoad()) {
         // 更新当前统计的数据
         update();
      }

      return totalConnections;
   }

   public int getIdleConnections()
   {
      if (shouldLoad()) {
         update();
      }

      return idleConnections;
   }

   public int getActiveConnections()
   {
      if (shouldLoad()) {
         update();
      }

      return activeConnections;
   }

   public int getPendingThreads()
   {
      if (shouldLoad()) {
         update();
      }

      return pendingThreads;
   }

   public int getMaxConnections() {
      if (shouldLoad()) {
         update();
      }

      return maxConnections;
   }

   public int getMinConnections() {
      if (shouldLoad()) {
         update();
      }

      return minConnections;
   }

   /**
    * 更新逻辑由子类实现
    */
   protected abstract void update();

   /**
    * 是否应该进行加载
    * @return
    */
   private boolean shouldLoad()
   {
      for (; ; ) {
          final long now = currentTime();
          final long reloadTime = reloadAt.get();
          // 如果需要重新加载的时间 大于当前时间就不需要加载 看来该值是设置在未来的某个时间点的
          if (reloadTime > now) {
              return false;
          }

          // 到了重新加载的时间了 同时设置下一次的重新加载时间
          else if (reloadAt.compareAndSet(reloadTime, plusMillis(now, timeoutMs))) {
              return true;
          }
      }
  }
}
