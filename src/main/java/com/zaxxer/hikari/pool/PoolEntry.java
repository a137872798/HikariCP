/*
 * Copyright (C) 2014 Brett Wooldridge
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
package com.zaxxer.hikari.pool;

import com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry;
import com.zaxxer.hikari.util.FastList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static com.zaxxer.hikari.util.ClockSource.*;

/**
 * Entry used in the ConcurrentBag to track Connection instances.
 * 该对象实现了 ConcurrentBag 类中实体接口
 * 每个被创建的connection 会被该对象包裹 同时 该对象内部有一个 维护statement 的 FastList
 * @author Brett Wooldridge
 */
final class PoolEntry implements IConcurrentBagEntry
{
   private static final Logger LOGGER = LoggerFactory.getLogger(PoolEntry.class);
   /**
    * 使用原子引用更新状态 (原子更新对象通常可以声明成常量 因为它是无状态的)
    */
   private static final AtomicIntegerFieldUpdater<PoolEntry> stateUpdater;

   /**
    * 每个entry 对象对应一个 连接
    */
   Connection connection;
   /**
    * 最后被访问的时间戳
    */
   long lastAccessed;
   /**
    * 最后被借用的时间戳  针对该对象从bag 中被某条线程获取
    */
   long lastBorrowed;

   @SuppressWarnings("FieldCanBeLocal")
   private volatile int state = 0;
   /**
    * 是否被驱逐
    */
   private volatile boolean evict;

   /**
    * 如果该对象设置了最大存活时间 那么在快要到达该时间时 会触发这个定时任务  为什么该对象要持有定时任务引用 因为关闭连接时是直接调用该对象的close 方法 那么
    * 同时要关闭定时任务 所以要持有引用
    */
   private volatile ScheduledFuture<?> endOfLife;

   /**
    * 存储会话的对象 因为该列表被访问的特殊性 所以能够构建不需要范围检查的list 用于提升性能
    */
   private final FastList<Statement> openStatements;
   /**
    * 该entry 关联的pool
    */
   private final HikariPool hikariPool;

   /**
    * 是否只读
    */
   private final boolean isReadOnly;
   /**
    * 是否自动提交
    */
   private final boolean isAutoCommit;

   static
   {
      stateUpdater = AtomicIntegerFieldUpdater.newUpdater(PoolEntry.class, "state");
   }

   PoolEntry(final Connection connection, final PoolBase pool, final boolean isReadOnly, final boolean isAutoCommit)
   {
      this.connection = connection;
      this.hikariPool = (HikariPool) pool;
      this.isReadOnly = isReadOnly;
      this.isAutoCommit = isAutoCommit;
      this.lastAccessed = currentTime();
      // 在创建一个 连接对象时(该对象在连接池中就是一个连接对象) 会同时初始化 该连接生成的 statement 容器
      this.openStatements = new FastList<>(Statement.class, 16);
   }

   /**
    * Release this entry back to the pool.
    * 归还连接(本对象) 到连接池中
    * @param lastAccessed last access time-stamp
    */
   void recycle(final long lastAccessed)
   {
      if (connection != null) {
         // 更新最后访问时间
         this.lastAccessed = lastAccessed;
         // 归还元素
         hikariPool.recycle(this);
      }
   }

   /**
    * Set the end of life {@link ScheduledFuture}.
    *
    * @param endOfLife this PoolEntry/Connection's end of life {@link ScheduledFuture}
    */
   void setFutureEol(final ScheduledFuture<?> endOfLife)
   {
      this.endOfLife = endOfLife;
   }

   /**
    * 初始化该对象时 conn 是jdbc 原生连接 而从pool中获取连接时 通过调用该方法返回一个 代理对象 应该就是重写 close 方法
    * 应该做了缓存 而不是每次都新建对象
    * @param leakTask 泄露检测任务   内部包含一个 携带栈轨迹的异常对象 定时任务执行时就是更新 栈轨迹信息
    * @param now 获取到entry 的时间
    * @return
    */
   Connection createProxyConnection(final ProxyLeakTask leakTask, final long now)
   {
      return ProxyFactory.getProxyConnection(this, connection, openStatements, leakTask, now, isReadOnly, isAutoCommit);
   }

   /**
    * 重置状态
    * @param proxyConnection
    * @param dirtyBits
    * @throws SQLException
    */
   void resetConnectionState(final ProxyConnection proxyConnection, final int dirtyBits) throws SQLException
   {
      hikariPool.resetConnectionState(connection, proxyConnection, dirtyBits);
   }

   String getPoolName()
   {
      return hikariPool.toString();
   }

   /**
    * 是否已经被驱逐
    * @return
    */
   boolean isMarkedEvicted()
   {
      return evict;
   }

   void markEvicted()
   {
      this.evict = true;
   }

   /**
    * evict 就是关闭连接
    * @param closureReason
    */
   void evict(final String closureReason)
   {
      hikariPool.closeConnection(this, closureReason);
   }

   /** Returns millis since lastBorrowed */
   long getMillisSinceBorrowed()
   {
      return elapsedMillis(lastBorrowed);
   }

   /** {@inheritDoc} */
   @Override
   public String toString()
   {
      final long now = currentTime();
      return connection
         + ", accessed " + elapsedDisplayString(lastAccessed, now) + " ago, "
         + stateToString();
   }

   // ***********************************************************************
   //                      IConcurrentBagEntry methods
   // ***********************************************************************

   /** {@inheritDoc} */
   @Override
   public int getState()
   {
      return stateUpdater.get(this);
   }

   /** {@inheritDoc} */
   @Override
   public boolean compareAndSet(int expect, int update)
   {
      return stateUpdater.compareAndSet(this, expect, update);
   }

   /** {@inheritDoc} */
   @Override
   public void setState(int update)
   {
      stateUpdater.set(this, update);
   }

   /**
    * 关闭连接
    * @return
    */
   Connection close()
   {
      ScheduledFuture<?> eol = endOfLife;
      // 如果定时任务关闭失败 只是打印日志 不能影响流程
      if (eol != null && !eol.isDone() && !eol.cancel(false)) {
         LOGGER.warn("{} - maxLifeTime expiration task cancellation unexpectedly returned false for connection {}", getPoolName(), connection);
      }

      Connection con = connection;
      connection = null;
      endOfLife = null;
      return con;
   }

   private String stateToString()
   {
      switch (state) {
      case STATE_IN_USE:
         return "IN_USE";
      case STATE_NOT_IN_USE:
         return "NOT_IN_USE";
      case STATE_REMOVED:
         return "REMOVED";
      case STATE_RESERVED:
         return "RESERVED";
      default:
         return "Invalid";
      }
   }
}
