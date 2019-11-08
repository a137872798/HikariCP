/*
 * Copyright (C) 2013, 2014 Brett Wooldridge
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

package com.zaxxer.hikari.util;

import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.concurrent.Semaphore;

/**
 * This class implements a lock that can be used to suspend and resume the pool.  It
 * also provides a faux implementation that is used when the feature is disabled that
 * hopefully gets fully "optimized away" by the JIT.
 * 该锁用于 悬挂和 恢复线程池  并且提供了一个空实现 在不使用时能够被 JIT 完全的优化
 * @author Brett Wooldridge
 */
public class SuspendResumeLock
{
   /**
    * faux(仿制的 等同于fake) 实现
    * 实际上使用基于false 的时候就应该用该静态变量  否则直接调用方法会出现空指针
    */
   public static final SuspendResumeLock FAUX_LOCK = new SuspendResumeLock(false) {
      @Override
      public void acquire() {}

      @Override
      public void release() {}

      @Override
      public void suspend() {}

      @Override
      public void resume() {}
   };

   /**
    * 许可证数量默认为 10000
    */
   private static final int MAX_PERMITS = 10000;
   /**
    * 内部维护了一个信号量
    */
   private final Semaphore acquisitionSemaphore;

   /**
    * Default constructor
    */
   public SuspendResumeLock()
   {
      this(true);
   }

   private SuspendResumeLock(final boolean createSemaphore)
   {
      // 根据情况 是否要设置 信号量
      acquisitionSemaphore = (createSemaphore ? new Semaphore(MAX_PERMITS, true) : null);
   }

   public void acquire() throws SQLException
   {
      if (acquisitionSemaphore.tryAcquire()) {
         return;
      }
      // 当没有门票时是否抛出异常
      else if (Boolean.getBoolean("com.zaxxer.hikari.throwIfSuspended")) {
         throw new SQLTransientException("The pool is currently suspended and configured to throw exceptions upon acquisition");
      }

      // 阻塞获取锁
      acquisitionSemaphore.acquireUninterruptibly();
   }

   public void release()
   {
      acquisitionSemaphore.release();
   }

   /**
    * 悬挂就是一次性使用完所有门票 而对应的 恢复就是 释放所有门票
    */
   public void suspend()
   {
      acquisitionSemaphore.acquireUninterruptibly(MAX_PERMITS);
   }

   public void resume()
   {
      acquisitionSemaphore.release(MAX_PERMITS);
   }
}
