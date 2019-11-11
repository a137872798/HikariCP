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

package com.zaxxer.hikari.pool;

import com.zaxxer.hikari.util.FastList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;

import static com.zaxxer.hikari.util.ClockSource.currentTime;

/**
 * This is the proxy class for java.sql.Connection.
 * 连接代理对象
 * @author Brett Wooldridge
 */
public abstract class ProxyConnection implements Connection
{
   // 一些状态位标识
   static final int DIRTY_BIT_READONLY   = 0b000001;
   static final int DIRTY_BIT_AUTOCOMMIT = 0b000010;
   static final int DIRTY_BIT_ISOLATION  = 0b000100;
   static final int DIRTY_BIT_CATALOG    = 0b001000;
   static final int DIRTY_BIT_NETTIMEOUT = 0b010000;
   static final int DIRTY_BIT_SCHEMA     = 0b100000;

   private static final Logger LOGGER;
   // 记录错误状态的 Set
   private static final Set<String> ERROR_STATES;
   private static final Set<Integer> ERROR_CODES;

   /**
    * 委托的实际对象
    */
   @SuppressWarnings("WeakerAccess")
   protected Connection delegate;

   /**
    * 一个 连接绑定到一个entry上  他们相互依赖
    */
   private final PoolEntry poolEntry;
   /**
    * 泄露检测对象
    */
   private final ProxyLeakTask leakTask;
   /**
    * 打开的 会话对象列表
    */
   private final FastList<Statement> openStatements;

   /**
    * 状态位标识
    */
   private int dirtyBits;
   /**
    * 最后访问时间
    */
   private long lastAccess;
   /**
    * 每当 statement 执行了某条语句后 该标识就会变成true 代表有需要提交的数据
    * 查询语句 也会修改该标识
    */
   private boolean isCommitStateDirty;

   private boolean isReadOnly;
   private boolean isAutoCommit;
   /**
    * 网络超时时间
    */
   private int networkTimeout;
   private int transactionIsolation;
   private String dbcatalog;
   private String dbschema;

   // static initializer
   static {
      LOGGER = LoggerFactory.getLogger(ProxyConnection.class);

      // jdbc 返回的异常状态
      ERROR_STATES = new HashSet<>();
      ERROR_STATES.add("0A000"); // FEATURE UNSUPPORTED
      ERROR_STATES.add("57P01"); // ADMIN SHUTDOWN
      ERROR_STATES.add("57P02"); // CRASH SHUTDOWN
      ERROR_STATES.add("57P03"); // CANNOT CONNECT NOW
      ERROR_STATES.add("01002"); // SQL92 disconnect error
      ERROR_STATES.add("JZ0C0"); // Sybase disconnect error
      ERROR_STATES.add("JZ0C1"); // Sybase disconnect error

      ERROR_CODES = new HashSet<>();
      ERROR_CODES.add(500150);
      ERROR_CODES.add(2399);
   }

   /**
    * 初始化代理连接
    * @param poolEntry
    * @param connection
    * @param openStatements
    * @param leakTask
    * @param now
    * @param isReadOnly
    * @param isAutoCommit
    */
   protected ProxyConnection(final PoolEntry poolEntry, final Connection connection, final FastList<Statement> openStatements, final ProxyLeakTask leakTask, final long now, final boolean isReadOnly, final boolean isAutoCommit) {
      this.poolEntry = poolEntry;
      this.delegate = connection;
      this.openStatements = openStatements;
      this.leakTask = leakTask;
      this.lastAccess = now;
      this.isReadOnly = isReadOnly;
      this.isAutoCommit = isAutoCommit;
   }

   /** {@inheritDoc} */
   @Override
   public final String toString()
   {
      return this.getClass().getSimpleName() + '@' + System.identityHashCode(this) + " wrapping " + delegate;
   }

   // ***********************************************************************
   //                     Connection State Accessors
   // ***********************************************************************

   final boolean getAutoCommitState()
   {
      return isAutoCommit;
   }

   final String getCatalogState()
   {
      return dbcatalog;
   }

   final String getSchemaState()
   {
      return dbschema;
   }

   final int getTransactionIsolationState()
   {
      return transactionIsolation;
   }

   final boolean getReadOnlyState()
   {
      return isReadOnly;
   }

   final int getNetworkTimeoutState()
   {
      return networkTimeout;
   }

   // ***********************************************************************
   //                          Internal methods
   // ***********************************************************************

   final PoolEntry getPoolEntry()
   {
      return poolEntry;
   }

   /**
    * 校验异常信息
    * @param sqle
    * @return
    */
   final SQLException checkException(SQLException sqle)
   {
      SQLException nse = sqle;
      // 只处理当前conn 对象不是 被关闭对象的情况
      for (int depth = 0; delegate != ClosedConnection.CLOSED_CONNECTION && nse != null && depth < 10; depth++) {
         // 获取 sql异常对应的 状态
         final String sqlState = nse.getSQLState();
         if (sqlState != null && sqlState.startsWith("08")
             || nse instanceof SQLTimeoutException
             || ERROR_STATES.contains(sqlState)
             || ERROR_CODES.contains(nse.getErrorCode())) {

            // broken connection
            LOGGER.warn("{} - Connection {} marked as broken because of SQLSTATE({}), ErrorCode({})",
                        poolEntry.getPoolName(), delegate, sqlState, nse.getErrorCode(), nse);
            // 将泄露任务关闭
            leakTask.cancel();
            // 通过 poolEntry 来关闭连接
            poolEntry.evict("(connection is broken)");
            // 将当前对象更改成 被关闭的连接对象
            delegate = ClosedConnection.CLOSED_CONNECTION;
         }
         else {
            // 否则尝试获取下一个异常 如果不存在异常了会返回null
            nse = nse.getNextException();
         }
      }

      return sqle;
   }

   /**
    * 将某个会话从 轨迹容器中移除
    * @param statement
    */
   final synchronized void untrackStatement(final Statement statement)
   {
      openStatements.remove(statement);
   }

   /**
    * 如果是自动提交不需要做处理 否则设置 待提交标识为 true
    */
   final void markCommitStateDirty()
   {
      if (isAutoCommit) {
         lastAccess = currentTime();
      }
      else {
         isCommitStateDirty = true;
      }
   }

   /**
    * 关闭泄露任务
    */
   void cancelLeakTask()
   {
      leakTask.cancel();
   }

   /**
    * 将某个会话添加到 轨迹容器中
    * @param statement
    * @param <T>
    * @return
    */
   private synchronized <T extends Statement> T trackStatement(final T statement)
   {
      openStatements.add(statement);

      return statement;
   }

   /**
    * 关闭会话
    */
   @SuppressWarnings("EmptyTryBlock")
   private synchronized void closeStatements()
   {
      final int size = openStatements.size();
      if (size > 0) {
         for (int i = 0; i < size && delegate != ClosedConnection.CLOSED_CONNECTION; i++) {
            // 被动触发 close 方法
            try (Statement ignored = openStatements.get(i)) {
               // automatic resource cleanup
            }
            catch (SQLException e) {
               LOGGER.warn("{} - Connection {} marked as broken because of an exception closing open statements during Connection.close()",
                           poolEntry.getPoolName(), delegate);
               leakTask.cancel();
               poolEntry.evict("(exception closing Statements during Connection.close())");
               delegate = ClosedConnection.CLOSED_CONNECTION;
            }
         }

         openStatements.clear();
      }
   }

   // **********************************************************************
   //              "Overridden" java.sql.Connection Methods
   // **********************************************************************

   /** {@inheritDoc} */
   @Override
   public final void close() throws SQLException
   {
      // Closing statements can cause connection eviction, so this must run before the conditional below
      // 关闭 openStatement 中所有会话
      closeStatements();

      if (delegate != ClosedConnection.CLOSED_CONNECTION) {
         leakTask.cancel();

         try {
            // 未开启自动提交时 关闭要对事务进行回滚
            if (isCommitStateDirty && !isAutoCommit) {
               delegate.rollback();
               lastAccess = currentTime();
               LOGGER.debug("{} - Executed rollback on connection {} due to dirty commit state on close().", poolEntry.getPoolName(), delegate);
            }

            // 将状态重置
            if (dirtyBits != 0) {
               poolEntry.resetConnectionState(this, dirtyBits);
               lastAccess = currentTime();
            }

            // jdbc相关的清理 可以不了解
            delegate.clearWarnings();
         }
         catch (SQLException e) {
            // when connections are aborted, exceptions are often thrown that should not reach the application
            // 当出现异常时 如果 该实体还没有被驱逐 触发 checkException  该方法内部会调用 evict
            if (!poolEntry.isMarkedEvicted()) {
               throw checkException(e);
            }
         }
         finally {
            delegate = ClosedConnection.CLOSED_CONNECTION;
            poolEntry.recycle(lastAccess);
         }
      }
   }

   /** {@inheritDoc} */
   @Override
   @SuppressWarnings("RedundantThrows")
   public boolean isClosed() throws SQLException
   {
      return (delegate == ClosedConnection.CLOSED_CONNECTION);
   }

   // 返回的都是代理对象

   /** {@inheritDoc} */
   @Override
   public Statement createStatement() throws SQLException
   {
      return ProxyFactory.getProxyStatement(this, trackStatement(delegate.createStatement()));
   }

   /** {@inheritDoc} */
   @Override
   public Statement createStatement(int resultSetType, int concurrency) throws SQLException
   {
      return ProxyFactory.getProxyStatement(this, trackStatement(delegate.createStatement(resultSetType, concurrency)));
   }

   /** {@inheritDoc} */
   @Override
   public Statement createStatement(int resultSetType, int concurrency, int holdability) throws SQLException
   {
      return ProxyFactory.getProxyStatement(this, trackStatement(delegate.createStatement(resultSetType, concurrency, holdability)));
   }


   /** {@inheritDoc} */
   @Override
   public CallableStatement prepareCall(String sql) throws SQLException
   {
      return ProxyFactory.getProxyCallableStatement(this, trackStatement(delegate.prepareCall(sql)));
   }

   /** {@inheritDoc} */
   @Override
   public CallableStatement prepareCall(String sql, int resultSetType, int concurrency) throws SQLException
   {
      return ProxyFactory.getProxyCallableStatement(this, trackStatement(delegate.prepareCall(sql, resultSetType, concurrency)));
   }

   /** {@inheritDoc} */
   @Override
   public CallableStatement prepareCall(String sql, int resultSetType, int concurrency, int holdability) throws SQLException
   {
      return ProxyFactory.getProxyCallableStatement(this, trackStatement(delegate.prepareCall(sql, resultSetType, concurrency, holdability)));
   }

   /** {@inheritDoc} */
   @Override
   public PreparedStatement prepareStatement(String sql) throws SQLException
   {
      return ProxyFactory.getProxyPreparedStatement(this, trackStatement(delegate.prepareStatement(sql)));
   }

   /** {@inheritDoc} */
   @Override
   public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException
   {
      return ProxyFactory.getProxyPreparedStatement(this, trackStatement(delegate.prepareStatement(sql, autoGeneratedKeys)));
   }

   /** {@inheritDoc} */
   @Override
   public PreparedStatement prepareStatement(String sql, int resultSetType, int concurrency) throws SQLException
   {
      return ProxyFactory.getProxyPreparedStatement(this, trackStatement(delegate.prepareStatement(sql, resultSetType, concurrency)));
   }

   /** {@inheritDoc} */
   @Override
   public PreparedStatement prepareStatement(String sql, int resultSetType, int concurrency, int holdability) throws SQLException
   {
      return ProxyFactory.getProxyPreparedStatement(this, trackStatement(delegate.prepareStatement(sql, resultSetType, concurrency, holdability)));
   }

   /** {@inheritDoc} */
   @Override
   public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException
   {
      return ProxyFactory.getProxyPreparedStatement(this, trackStatement(delegate.prepareStatement(sql, columnIndexes)));
   }

   /** {@inheritDoc} */
   @Override
   public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException
   {
      return ProxyFactory.getProxyPreparedStatement(this, trackStatement(delegate.prepareStatement(sql, columnNames)));
   }

   /** {@inheritDoc} */
   @Override
   public DatabaseMetaData getMetaData() throws SQLException
   {
      markCommitStateDirty();
      return ProxyFactory.getProxyDatabaseMetaData(this, delegate.getMetaData());
   }

   /**
    * 提交事务后 将当前 dirty 标记去除
    * {@inheritDoc}
    */
   @Override
   public void commit() throws SQLException
   {
      delegate.commit();
      isCommitStateDirty = false;
      lastAccess = currentTime();
   }

   /** {@inheritDoc} */
   @Override
   public void rollback() throws SQLException
   {
      delegate.rollback();
      isCommitStateDirty = false;
      lastAccess = currentTime();
   }

   /** {@inheritDoc} */
   @Override
   public void rollback(Savepoint savepoint) throws SQLException
   {
      delegate.rollback(savepoint);
      isCommitStateDirty = false;
      lastAccess = currentTime();
   }

   /** {@inheritDoc} 修改标识的同时会设置 bits */
   @Override
   public void setAutoCommit(boolean autoCommit) throws SQLException
   {
      delegate.setAutoCommit(autoCommit);
      isAutoCommit = autoCommit;
      dirtyBits |= DIRTY_BIT_AUTOCOMMIT;
   }

   /** {@inheritDoc} */
   @Override
   public void setReadOnly(boolean readOnly) throws SQLException
   {
      delegate.setReadOnly(readOnly);
      isReadOnly = readOnly;
      isCommitStateDirty = false;
      dirtyBits |= DIRTY_BIT_READONLY;
   }

   /** {@inheritDoc} */
   @Override
   public void setTransactionIsolation(int level) throws SQLException
   {
      delegate.setTransactionIsolation(level);
      transactionIsolation = level;
      dirtyBits |= DIRTY_BIT_ISOLATION;
   }

   /** {@inheritDoc} */
   @Override
   public void setCatalog(String catalog) throws SQLException
   {
      delegate.setCatalog(catalog);
      dbcatalog = catalog;
      dirtyBits |= DIRTY_BIT_CATALOG;
   }

   /** {@inheritDoc} */
   @Override
   public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException
   {
      delegate.setNetworkTimeout(executor, milliseconds);
      networkTimeout = milliseconds;
      dirtyBits |= DIRTY_BIT_NETTIMEOUT;
   }

   /** {@inheritDoc} */
   @Override
   public void setSchema(String schema) throws SQLException
   {
      delegate.setSchema(schema);
      dbschema = schema;
      dirtyBits |= DIRTY_BIT_SCHEMA;
   }

   /** {@inheritDoc} wrapperFor是 jdbc 内置的方法 先忽略 判断该对象是否是 指定参数的包装器 */
   @Override
   public final boolean isWrapperFor(Class<?> iface) throws SQLException
   {
      return iface.isInstance(delegate) || (delegate != null && delegate.isWrapperFor(iface));
   }

   /** {@inheritDoc} */
   @Override
   @SuppressWarnings("unchecked")
   public final <T> T unwrap(Class<T> iface) throws SQLException
   {
      if (iface.isInstance(delegate)) {
         return (T) delegate;
      }
      else if (delegate != null) {
          return delegate.unwrap(iface);
      }

      throw new SQLException("Wrapped connection is not an instance of " + iface);
   }

   // **********************************************************************
   //                         Private classes
   // **********************************************************************

   /**
    * 被关闭的连接对象
    */
   private static final class ClosedConnection
   {
      // 获取 Closed_connection 只会触发一次 getClosedConnection()
      static final Connection CLOSED_CONNECTION = getClosedConnection();

      private static Connection getClosedConnection()
      {
         // 重写close 相关的方法 首先 isClosed 返回 true 其次 close 不做任何操作  该对象内部没有维护 connection 也就是其他方法被屏蔽了
         // (尝试调用其他方法直接返回 SQLException("Connection is closed"))
         // 而一般使用动态代理的套路都是内部维护一个真实的对象引用 然后一般的方法都通过委托 交给内部对象 某些需要特殊处理的方法才加额外逻辑
         InvocationHandler handler = (proxy, method, args) -> {
            final String methodName = method.getName();
            if ("isClosed".equals(methodName)) {
               return Boolean.TRUE;
            }
            else if ("isValid".equals(methodName)) {
               return Boolean.FALSE;
            }
            if ("abort".equals(methodName)) {
               return Void.TYPE;
            }
            if ("close".equals(methodName)) {
               return Void.TYPE;
            }
            else if ("toString".equals(methodName)) {
               return ClosedConnection.class.getCanonicalName();
            }

            throw new SQLException("Connection is closed");
         };

         return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class[] { Connection.class }, handler);
      }
   }
}
