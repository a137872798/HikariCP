/*
 * Copyright (C) 2013 Brett Wooldridge
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

import java.lang.reflect.Constructor;
import java.util.Locale;
import java.util.concurrent.*;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * 一个简单的工具类
 * @author Brett Wooldridge
 */
public final class UtilityElf
{
   /**
    * 如果是 空 或者null 返回null 否则返回 去空后的字符串
    * @return null if string is null or empty
   */
   public static String getNullIfEmpty(final String text)
   {
      return text == null ? null : text.trim().isEmpty() ? null : text.trim();
   }

   /**
    * Sleep and suppress InterruptedException (but re-signal it).
    * 让线程沉睡指定时间
    * @param millis the number of milliseconds to sleep
    */
   public static void quietlySleep(final long millis)
   {
      try {
         Thread.sleep(millis);
      }
      catch (InterruptedException e) {
         // I said be quiet!
         currentThread().interrupt();
      }
   }

   /**
    * Checks whether an object is an instance of given type without throwing exception when the class is not loaded.
    * @param obj the object to check
    * @param className String class
    * @return true if object is assignable from the type, false otherwise or when the class cannot be loaded
    */
   public static boolean safeIsAssignableFrom(Object obj, String className) {
      try {
         Class<?> clazz = Class.forName(className);
         return clazz.isAssignableFrom(obj.getClass());
      } catch (ClassNotFoundException ignored) {
         return false;
      }
   }

   /**
    * Create and instance of the specified class using the constructor matching the specified
    * arguments.
    *
    * @param <T> the class type
    * @param className the name of the class to instantiate
    * @param clazz a class to cast the result as
    * @param args arguments to a constructor
    * @return an instance of the specified class
    */
   public static <T> T createInstance(final String className, final Class<T> clazz, final Object... args)
   {
      if (className == null) {
         return null;
      }

      try {
         // 通过类加载器 获取 class 信息
         Class<?> loaded = UtilityElf.class.getClassLoader().loadClass(className);
         if (args.length == 0) {
            // 直接实例化对象
            return clazz.cast(loaded.newInstance());
         }

         // 代表该对象需要参数才能实例化
         Class<?>[] argClasses = new Class<?>[args.length];
         for (int i = 0; i < args.length; i++) {
            argClasses[i] = args[i].getClass();
         }
         Constructor<?> constructor = loaded.getConstructor(argClasses);
         return clazz.cast(constructor.newInstance(args));
      }
      catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   /**
    * Create a ThreadPoolExecutor.
    *
    * @param queueSize the queue size
    * @param threadName the thread name
    * @param threadFactory an optional ThreadFactory
    * @param policy the RejectedExecutionHandler policy
    * @return a ThreadPoolExecutor
    */
   public static ThreadPoolExecutor createThreadPoolExecutor(final int queueSize, final String threadName, ThreadFactory threadFactory, final RejectedExecutionHandler policy)
   {
      if (threadFactory == null) {
         threadFactory = new DefaultThreadFactory(threadName, true);
      }

      LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(queueSize);
      ThreadPoolExecutor executor = new ThreadPoolExecutor(1 /*core*/, 1 /*max*/, 5 /*keepalive*/, SECONDS, queue, threadFactory, policy);
      executor.allowCoreThreadTimeOut(true);
      return executor;
   }

   /**
    * Create a ThreadPoolExecutor.
    *
    * @param queue the BlockingQueue to use
    * @param threadName the thread name
    * @param threadFactory an optional ThreadFactory
    * @param policy the RejectedExecutionHandler policy
    * @return a ThreadPoolExecutor
    */
   public static ThreadPoolExecutor createThreadPoolExecutor(final BlockingQueue<Runnable> queue, final String threadName, ThreadFactory threadFactory, final RejectedExecutionHandler policy)
   {
      if (threadFactory == null) {
         threadFactory = new DefaultThreadFactory(threadName, true);
      }

      ThreadPoolExecutor executor = new ThreadPoolExecutor(1 /*core*/, 1 /*max*/, 5 /*keepalive*/, SECONDS, queue, threadFactory, policy);
      // 设置该属性后 核心线程也允许被回收
      executor.allowCoreThreadTimeOut(true);
      return executor;
   }

   // ***********************************************************************
   //                       Misc. public methods
   // ***********************************************************************

   /**
    * Get the int value of a transaction isolation level by name.
    * 根据事务隔离级别名称 获取对象的int 值
    * @param transactionIsolationName the name of the transaction isolation level
    * @return the int value of the isolation level or -1
    */
   public static int getTransactionIsolation(final String transactionIsolationName)
   {
      if (transactionIsolationName != null) {
         try {
            // use the english locale to avoid the infamous turkish locale bug   英文+大写是为了避免某种bug
            final String upperCaseIsolationLevelName = transactionIsolationName.toUpperCase(Locale.ENGLISH);
            // 从枚举类中找到对应的隔离级别
            return IsolationLevel.valueOf(upperCaseIsolationLevelName).getLevelId();
         } catch (IllegalArgumentException e) {
            // legacy support for passing an integer version of the isolation level
            try {
               // 如果 传入的就是数字 也尝试匹配枚举
               final int level = Integer.parseInt(transactionIsolationName);
               for (IsolationLevel iso : IsolationLevel.values()) {
                  if (iso.getLevelId() == level) {
                     return iso.getLevelId();
                  }
               }

               throw new IllegalArgumentException("Invalid transaction isolation value: " + transactionIsolationName);
            }
            catch (NumberFormatException nfe) {
               throw new IllegalArgumentException("Invalid transaction isolation value: " + transactionIsolationName, nfe);
            }
         }
      }

      return -1;
   }

   public static final class DefaultThreadFactory implements ThreadFactory {

      private final String threadName;
      private final boolean daemon;

      public DefaultThreadFactory(String threadName, boolean daemon) {
         this.threadName = threadName;
         this.daemon = daemon;
      }

      /**
       * 默认线程工厂 每次都会创建一条新线程
       * @param r
       * @return
       */
      @Override
      public Thread newThread(Runnable r) {
         Thread thread = new Thread(r, threadName);
         thread.setDaemon(daemon);
         return thread;
      }
   }
}
