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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.metrics.IMetricsTracker;
import com.zaxxer.hikari.pool.HikariPool.PoolInitializationException;
import com.zaxxer.hikari.util.DriverDataSource;
import com.zaxxer.hikari.util.PropertyElf;
import com.zaxxer.hikari.util.UtilityElf;
import com.zaxxer.hikari.util.UtilityElf.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;

import static com.zaxxer.hikari.pool.ProxyConnection.*;
import static com.zaxxer.hikari.util.ClockSource.*;
import static com.zaxxer.hikari.util.UtilityElf.createInstance;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * 连接池的基类
 */
abstract class PoolBase
{
   private final Logger logger = LoggerFactory.getLogger(PoolBase.class);

   /**
    * 内部维护了 hikari的配置对象  需要的配置可以直接从这里获取
    */
   public final HikariConfig config;
   /**
    * 轨迹测量代理对象
    */
   IMetricsTrackerDelegate metricsTracker;

   /**
    * 连接池的名称
    */
   protected final String poolName;

   /**
    * 目录
    */
   volatile String catalog;
   /**
    * 当连接失败时的异常
    */
   final AtomicReference<Exception> lastConnectionFailure;

   /**
    * 连接超时时间
    */
   long connectionTimeout;
   /**
    * 校验超时时间
    */
   long validationTimeout;

   private static final String[] RESET_STATES = {"readOnly", "autoCommit", "isolation", "catalog", "netTimeout", "schema"};
   /**
    * 代表当前未初始化
    */
   private static final int UNINITIALIZED = -1;
   private static final int TRUE = 1;
   private static final int FALSE = 0;

   private int networkTimeout;
   private int isNetworkTimeoutSupported;
   private int isQueryTimeoutSupported;
   private int defaultTransactionIsolation;
   private int transactionIsolation;
   private Executor netTimeoutExecutor;
   private DataSource dataSource;

   private final String schema;
   private final boolean isReadOnly;
   private final boolean isAutoCommit;

   /**
    * 是否通过 jdbcApi 来测量连接是否有效
    */
   private final boolean isUseJdbc4Validation;
   private final boolean isIsolateInternalQueries;

   private volatile boolean isValidChecked;

   /**
    * 通过 hikariConf 进行初始化
    * 总结该基类的构造函数通过config 初始化相关属性 并创建了dataSource 对象
    * @param config
    */
   PoolBase(final HikariConfig config)
   {
      this.config = config;

      // 网络超时时间默认为-1
      this.networkTimeout = UNINITIALIZED;
      // 获取目录属性
      this.catalog = config.getCatalog();
      this.schema = config.getSchema();
      this.isReadOnly = config.isReadOnly();
      this.isAutoCommit = config.isAutoCommit();
      // 将设置的 事务隔离级别转换成int  如果传入的格式错误会抛出异常
      this.transactionIsolation = UtilityElf.getTransactionIsolation(config.getTransactionIsolation());

      this.isQueryTimeoutSupported = UNINITIALIZED;
      this.isNetworkTimeoutSupported = UNINITIALIZED;
      // 是否启用连接测试
      this.isUseJdbc4Validation = config.getConnectionTestQuery() == null;
      this.isIsolateInternalQueries = config.isIsolateInternalQueries();

      this.poolName = config.getPoolName();
      this.connectionTimeout = config.getConnectionTimeout();
      this.validationTimeout = config.getValidationTimeout();
      this.lastConnectionFailure = new AtomicReference<>();

      // 利用 dataSourceClassName 或者 jdbcUrl 初始化 dataSource
      initializeDataSource();
   }

   /** {@inheritDoc} */
   @Override
   public String toString()
   {
      return poolName;
   }

   abstract void recycle(final PoolEntry poolEntry);

   // ***********************************************************************
   //                           JDBC methods
   // ***********************************************************************

   /**
    * 真正关闭连接对象  打印异常日志
    * @param connection
    * @param closureReason
    */
   void quietlyCloseConnection(final Connection connection, final String closureReason)
   {
      if (connection != null) {
         try {
            logger.debug("{} - Closing connection {}: {}", poolName, connection, closureReason);

            try {
               // 更新网络超时时间 （支持该属性的前提下）
               setNetworkTimeout(connection, SECONDS.toMillis(15));
            }
            catch (SQLException e) {
               // ignore
            }
            finally {
               connection.close(); // continue with the close even if setNetworkTimeout() throws
            }
         }
         catch (Exception e) {
            logger.debug("{} - Closing connection {} failed", poolName, connection, e);
         }
      }
   }

   /**
    * 判断当前连接是否存活
    * @param connection
    * @return
    */
   boolean isConnectionAlive(final Connection connection)
   {
      try {
         try {
            // 这里应该是防止 超时时间比校验时间要短
            setNetworkTimeout(connection, validationTimeout);

            // 以秒为单位
            final int validationSeconds = (int) Math.max(1000L, validationTimeout) / 1000;

            if (isUseJdbc4Validation) {
               // 查看当前连接是否可以 最多阻塞传入的时间
               return connection.isValid(validationSeconds);
            }

            // 执行一条测试语句 用来判断连接是否可用
            try (Statement statement = connection.createStatement()) {
               // 难道支持网络超时的情况下就不会因为查询超时而抛出失败吗???
               if (isNetworkTimeoutSupported != TRUE) {
                  setQueryTimeout(statement, validationSeconds);
               }

               statement.execute(config.getConnectionTestQuery());
            }
         }
         finally {
            // 还原网络超时时间
            setNetworkTimeout(connection, networkTimeout);

            // 回滚 测试语句造成的影响
            if (isIsolateInternalQueries && !isAutoCommit) {
               connection.rollback();
            }
         }

         return true;
      }
      catch (Exception e) {
         lastConnectionFailure.set(e);
         logger.warn("{} - Failed to validate connection {} ({}). Possibly consider using a shorter maxLifetime value.",
                     poolName, connection, e.getMessage());
         return false;
      }
   }

   Exception getLastConnectionFailure()
   {
      return lastConnectionFailure.get();
   }

   public DataSource getUnwrappedDataSource()
   {
      return dataSource;
   }

   // ***********************************************************************
   //                         PoolEntry methods
   // ***********************************************************************

   /**
    * 通过创建一个新连接来初始化 poolEntry
    * @return
    * @throws Exception
    */
   PoolEntry newPoolEntry() throws Exception
   {
      return new PoolEntry(newConnection(), this, isReadOnly, isAutoCommit);
   }

   /**
    * 重置连接状态
    * @param connection
    * @param proxyConnection
    * @param dirtyBits
    * @throws SQLException
    */
   void resetConnectionState(final Connection connection, final ProxyConnection proxyConnection, final int dirtyBits) throws SQLException
   {
      int resetBits = 0;

      if ((dirtyBits & DIRTY_BIT_READONLY) != 0 && proxyConnection.getReadOnlyState() != isReadOnly) {
         connection.setReadOnly(isReadOnly);
         resetBits |= DIRTY_BIT_READONLY;
      }

      if ((dirtyBits & DIRTY_BIT_AUTOCOMMIT) != 0 && proxyConnection.getAutoCommitState() != isAutoCommit) {
         connection.setAutoCommit(isAutoCommit);
         resetBits |= DIRTY_BIT_AUTOCOMMIT;
      }

      if ((dirtyBits & DIRTY_BIT_ISOLATION) != 0 && proxyConnection.getTransactionIsolationState() != transactionIsolation) {
         connection.setTransactionIsolation(transactionIsolation);
         resetBits |= DIRTY_BIT_ISOLATION;
      }

      if ((dirtyBits & DIRTY_BIT_CATALOG) != 0 && catalog != null && !catalog.equals(proxyConnection.getCatalogState())) {
         connection.setCatalog(catalog);
         resetBits |= DIRTY_BIT_CATALOG;
      }

      if ((dirtyBits & DIRTY_BIT_NETTIMEOUT) != 0 && proxyConnection.getNetworkTimeoutState() != networkTimeout) {
         setNetworkTimeout(connection, networkTimeout);
         resetBits |= DIRTY_BIT_NETTIMEOUT;
      }

      if ((dirtyBits & DIRTY_BIT_SCHEMA) != 0 && schema != null && !schema.equals(proxyConnection.getSchemaState())) {
         connection.setSchema(schema);
         resetBits |= DIRTY_BIT_SCHEMA;
      }

      if (resetBits != 0 && logger.isDebugEnabled()) {
         logger.debug("{} - Reset ({}) on connection {}", poolName, stringFromResetBits(resetBits), connection);
      }
   }

   /**
    * 终止网络超时执行器
    */
   void shutdownNetworkTimeoutExecutor()
   {
      if (netTimeoutExecutor instanceof ThreadPoolExecutor) {
         ((ThreadPoolExecutor) netTimeoutExecutor).shutdownNow();
      }
   }

   /**
    * 获取登录超时时间 默认5秒
    * @return
    */
   long getLoginTimeout()
   {
      try {
         return (dataSource != null) ? dataSource.getLoginTimeout() : SECONDS.toSeconds(5);
      } catch (SQLException e) {
         return SECONDS.toSeconds(5);
      }
   }

   // ***********************************************************************
   //                       JMX methods
   // ***********************************************************************

   /**
    * Register MBeans for HikariConfig and HikariPool.
    * 该方法是关于 mBean 的 就是一个可以通过 java kit 直接操作类 不影响 连接池的核心功能 先忽略
    * @param hikariPool a HikariPool instance
    */
   void handleMBeans(final HikariPool hikariPool, final boolean register)
   {
      if (!config.isRegisterMbeans()) {
         return;
      }

      try {
         final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

         final ObjectName beanConfigName = new ObjectName("com.zaxxer.hikari:type=PoolConfig (" + poolName + ")");
         final ObjectName beanPoolName = new ObjectName("com.zaxxer.hikari:type=Pool (" + poolName + ")");
         if (register) {
            if (!mBeanServer.isRegistered(beanConfigName)) {
               mBeanServer.registerMBean(config, beanConfigName);
               mBeanServer.registerMBean(hikariPool, beanPoolName);
            } else {
               logger.error("{} - JMX name ({}) is already registered.", poolName, poolName);
            }
         }
         else if (mBeanServer.isRegistered(beanConfigName)) {
            mBeanServer.unregisterMBean(beanConfigName);
            mBeanServer.unregisterMBean(beanPoolName);
         }
      }
      catch (Exception e) {
         logger.warn("{} - Failed to {} management beans.", poolName, (register ? "register" : "unregister"), e);
      }
   }

   // ***********************************************************************
   //                          Private methods
   // ***********************************************************************

   /**
    * Create/initialize the underlying DataSource.
    * 初始化 dataSource
    */
   private void initializeDataSource()
   {
      // 从config 中 获取dataSource 需要的属性
      final String jdbcUrl = config.getJdbcUrl();
      final String username = config.getUsername();
      final String password = config.getPassword();
      final String dsClassName = config.getDataSourceClassName();
      final String driverClassName = config.getDriverClassName();
      final String dataSourceJNDI = config.getDataSourceJNDI();
      final Properties dataSourceProperties = config.getDataSourceProperties();

      // 官方文档的介绍是 推荐使用 dataSourceClassName 进行初始化 而针对 setDataSource 一般是通过 IOC 框架 比如spring 直接注入数据源的
      DataSource ds = config.getDataSource();
      if (dsClassName != null && ds == null) {
         ds = createInstance(dsClassName, DataSource.class);
         // 从 配置文件中将 dataSource 相关的属性覆盖到 生成的对象中 如果dataSource 是通过IOC 框架注入进来的 就不需要更改属性了
         PropertyElf.setTargetFromProperties(ds, dataSourceProperties);
      }
      // 如果存在 jdbcUrl 那么就创建 基于驱动的 dataSource （首先尝试加载className 为 driverClassName 的类 失败后尝试使用 jdbcUrl 加载类）
      else if (jdbcUrl != null && ds == null) {
         ds = new DriverDataSource(jdbcUrl, driverClassName, dataSourceProperties, username, password);
      }
      // 使用 JNDI 查找dataSource 可以理解为 javaEE 的一种规范 从应用服务器(tomcat) 读取dataSource相关配置
      // 先忽略这种方式  主要是看 hikari快在哪里
      else if (dataSourceJNDI != null && ds == null) {
         try {
            // 该对象具备 直接从 j2EE 应用服务器读取指定配置并生成 dataSource的能力
            InitialContext ic = new InitialContext();
            ds = (DataSource) ic.lookup(dataSourceJNDI);
         } catch (NamingException e) {
            throw new PoolInitializationException(e);
         }
      }


      if (ds != null) {
         // 当 dataSource 成功初始化后 设置登录到db 的超时时间
         setLoginTimeout(ds);
         // 创建网络超时执行器 这个线程池是为了什么用的
         createNetworkTimeoutExecutor(ds, dsClassName, jdbcUrl);
      }

      this.dataSource = ds;
   }

   /**
    * Obtain connection from data source.
    * 核心流程就是通过dataSource获取一条 连接
    * 之后将属性设置到connection 后 通过一次与数据库的交互 更改设置
    * @return a Connection connection
    */
   private Connection newConnection() throws Exception
   {
      final long start = currentTime();

      Connection connection = null;
      try {
         String username = config.getUsername();
         String password = config.getPassword();

         // dataSource 是通过 driverClassName 或者一些其他东西获取的
         connection = (username == null) ? dataSource.getConnection() : dataSource.getConnection(username, password);

         // 加工连接
         setupConnection(connection);
         // 因为本次创建连接成功了 就将异常引用置空
         lastConnectionFailure.set(null);
         return connection;
      }
      catch (Exception e) {
         if (connection != null) {
            // 如果出现异常则关闭连接
            quietlyCloseConnection(connection, "(Failed to create/setup connection)");
         }
         else if (getLastConnectionFailure() == null) {
            logger.debug("{} - Failed to create/setup connection: {}", poolName, e.getMessage());
         }

         lastConnectionFailure.set(e);
         throw e;
      }
      finally {
         // tracker will be null during failFast check
         if (metricsTracker != null) {
            metricsTracker.recordConnectionCreated(elapsedMillis(start));
         }
      }
   }

   /**
    * Setup a connection initial state.
    * 设置连接对象初始状态
    * @param connection a Connection
    * @throws ConnectionSetupException thrown if any exception is encountered
    */
   private void setupConnection(final Connection connection) throws ConnectionSetupException
   {
      try {
         // 如果没有设置网络超时时间
         if (networkTimeout == UNINITIALIZED) {
            // 设置校验时间 并返回 默认的网络超时时间
            networkTimeout = getAndSetNetworkTimeout(connection, validationTimeout);
         }
         else {
            // 不需要记录网络超时时间
            setNetworkTimeout(connection, validationTimeout);
         }

         // 开始设置各种初始化参数
         if (connection.isReadOnly() != isReadOnly) {
            connection.setReadOnly(isReadOnly);
         }

         if (connection.getAutoCommit() != isAutoCommit) {
            connection.setAutoCommit(isAutoCommit);
         }

         checkDriverSupport(connection);

         if (transactionIsolation != defaultTransactionIsolation) {
            connection.setTransactionIsolation(transactionIsolation);
         }

         if (catalog != null) {
            connection.setCatalog(catalog);
         }

         if (schema != null) {
            connection.setSchema(schema);
         }

         // 看来执行sql 才能让 这些配置生效
         executeSql(connection, config.getConnectionInitSql(), true);

         // 还原网络超时时间
         setNetworkTimeout(connection, networkTimeout);
      }
      catch (SQLException e) {
         throw new ConnectionSetupException(e);
      }
   }

   /**
    * Execute isValid() or connection test query.
    * 检查驱动是否支持某些属性
    * @param connection a Connection to check
    */
   private void checkDriverSupport(final Connection connection) throws SQLException
   {
      // 如果 还没有检查过
      if (!isValidChecked) {
         checkValidationSupport(connection);
         checkDefaultIsolation(connection);

         isValidChecked = true;
      }
   }

   /**
    * Check whether Connection.isValid() is supported, or that the user has test query configured.
    *
    * @param connection a Connection to check
    * @throws SQLException rethrown from the driver
    */
   private void checkValidationSupport(final Connection connection) throws SQLException
   {
      try {
         // 如果支持 jdbc检查 直接调用 isValid
         if (isUseJdbc4Validation) {
            connection.isValid(1);
         }
         else {
            // 否则执行 测试用sql
            executeSql(connection, config.getConnectionTestQuery(), false);
         }
      }
      catch (Exception | AbstractMethodError e) {
         logger.error("{} - Failed to execute{} connection test query ({}).", poolName, (isUseJdbc4Validation ? " isValid() for connection, configure" : ""), e.getMessage());
         throw e;
      }
   }

   /**
    * Check the default transaction isolation of the Connection.
    * 检查默认隔离级别
    * @param connection a Connection to check
    * @throws SQLException rethrown from the driver
    */
   private void checkDefaultIsolation(final Connection connection) throws SQLException
   {
      try {
         defaultTransactionIsolation = connection.getTransactionIsolation();
         if (transactionIsolation == -1) {
            transactionIsolation = defaultTransactionIsolation;
         }
      }
      catch (SQLException e) {
         logger.warn("{} - Default transaction isolation level detection failed ({}).", poolName, e.getMessage());
         if (e.getSQLState() != null && !e.getSQLState().startsWith("08")) {
            throw e;
         }
      }
   }

   /**
    * Set the query timeout, if it is supported by the driver.
    * 设置查询超时时间
    * @param statement a statement to set the query timeout on
    * @param timeoutSec the number of seconds before timeout
    */
   private void setQueryTimeout(final Statement statement, final int timeoutSec)
   {
      // 支持或者未设置
      if (isQueryTimeoutSupported != FALSE) {
         try {
            statement.setQueryTimeout(timeoutSec);
            isQueryTimeoutSupported = TRUE;
         }
         catch (Exception e) {
            if (isQueryTimeoutSupported == UNINITIALIZED) {
               isQueryTimeoutSupported = FALSE;
               logger.info("{} - Failed to set query timeout for statement. ({})", poolName, e.getMessage());
            }
         }
      }
   }

   /**
    * Set the network timeout, if <code>isUseNetworkTimeout</code> is <code>true</code> and the
    * driver supports it.  Return the pre-existing value of the network timeout.
    * 该方法 大体就是设置一个新值 作为网络超时时间  并且返回旧值
    * @param connection the connection to set the network timeout on
    * @param timeoutMs the number of milliseconds before timeout
    * @return the pre-existing network timeout value
    */
   private int getAndSetNetworkTimeout(final Connection connection, final long timeoutMs)
   {
      // 有可能是 不支持 也有可能是 UNINITIALIZED(未初始化)
      if (isNetworkTimeoutSupported != FALSE) {
         try {
            // 获取conn 原始的超时时间
            final int originalTimeout = connection.getNetworkTimeout();
            // 设置成校验超时时间 jdbc原生api 不细看
            connection.setNetworkTimeout(netTimeoutExecutor, (int) timeoutMs);
            isNetworkTimeoutSupported = TRUE;
            return originalTimeout;
         }
         catch (Exception | AbstractMethodError e) {
            if (isNetworkTimeoutSupported == UNINITIALIZED) {
               isNetworkTimeoutSupported = FALSE;

               logger.info("{} - Driver does not support get/set network timeout for connections. ({})", poolName, e.getMessage());
               if (validationTimeout < SECONDS.toMillis(1)) {
                  logger.warn("{} - A validationTimeout of less than 1 second cannot be honored on drivers without setNetworkTimeout() support.", poolName);
               }
               else if (validationTimeout % SECONDS.toMillis(1) != 0) {
                  logger.warn("{} - A validationTimeout with fractional second granularity cannot be honored on drivers without setNetworkTimeout() support.", poolName);
               }
            }
         }
      }

      return 0;
   }

   /**
    * Set the network timeout, if <code>isUseNetworkTimeout</code> is <code>true</code> and the
    * driver supports it.
    *
    * @param connection the connection to set the network timeout on
    * @param timeoutMs the number of milliseconds before timeout
    * @throws SQLException throw if the connection.setNetworkTimeout() call throws
    */
   private void setNetworkTimeout(final Connection connection, final long timeoutMs) throws SQLException
   {
      if (isNetworkTimeoutSupported == TRUE) {
         connection.setNetworkTimeout(netTimeoutExecutor, (int) timeoutMs);
      }
   }

   /**
    * Execute the user-specified init SQL.
    * 执行sql 语句
    * @param connection the connection to initialize
    * @param sql the SQL to execute
    * @param isCommit whether to commit the SQL after execution or not
    * @throws SQLException throws if the init SQL execution fails
    */
   private void executeSql(final Connection connection, final String sql, final boolean isCommit) throws SQLException
   {
      if (sql != null) {
         try (Statement statement = connection.createStatement()) {
            // connection was created a few milliseconds before, so set query timeout is omitted (we assume it will succeed)
            statement.execute(sql);
         }

         // 根据本次是否要提交
         if (isIsolateInternalQueries && !isAutoCommit) {
            if (isCommit) {
               connection.commit();
            }
            else {
               connection.rollback();
            }
         }
      }
   }

   /**
    * 创建网络超时执行器
    * @param dataSource   数据源对象
    * @param dsClassName   数据源类名
    * @param jdbcUrl
    */
   private void createNetworkTimeoutExecutor(final DataSource dataSource, final String dsClassName, final String jdbcUrl)
   {
      // Temporary hack for MySQL issue: http://bugs.mysql.com/bug.php?id=75615
      // 如果是 mysql驱动
      if ((dsClassName != null && dsClassName.contains("Mysql")) ||
          (jdbcUrl != null && jdbcUrl.contains("mysql")) ||
          (dataSource != null && dataSource.getClass().getName().contains("Mysql"))) {
         // 实际上就是同步执行
         netTimeoutExecutor = new SynchronousExecutor();
      }
      else {
         ThreadFactory threadFactory = config.getThreadFactory();
         threadFactory = threadFactory != null ? threadFactory : new DefaultThreadFactory(poolName + " network timeout executor", true);
         ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool(threadFactory);
         executor.setKeepAliveTime(15, SECONDS);
         // 允许核心线程超时释放
         executor.allowCoreThreadTimeOut(true);
         netTimeoutExecutor = executor;
      }
   }

   /**
    * Set the loginTimeout on the specified DataSource.
    * 设置登录 db 的超时时间
    * @param dataSource the DataSource
    */
   private void setLoginTimeout(final DataSource dataSource)
   {
      if (connectionTimeout != Integer.MAX_VALUE) {
         try {
            dataSource.setLoginTimeout(Math.max(1, (int) MILLISECONDS.toSeconds(500L + connectionTimeout)));
         }
         catch (Exception e) {
            logger.info("{} - Failed to set login timeout for data source. ({})", poolName, e.getMessage());
         }
      }
   }

   /**
    * This will create a string for debug logging. Given a set of "reset bits", this
    * method will return a concatenated string, for example:
    *
    * Input : 0b00110
    * Output: "autoCommit, isolation"
    *
    * @param bits a set of "reset bits"
    * @return a string of which states were reset
    */
   private String stringFromResetBits(final int bits)
   {
      final StringBuilder sb = new StringBuilder();
      for (int ndx = 0; ndx < RESET_STATES.length; ndx++) {
         if ( (bits & (0b1 << ndx)) != 0) {
            sb.append(RESET_STATES[ndx]).append(", ");
         }
      }

      sb.setLength(sb.length() - 2);  // trim trailing comma
      return sb.toString();
   }

   // ***********************************************************************
   //                      Private Static Classes
   // ***********************************************************************

   static class ConnectionSetupException extends Exception
   {
      private static final long serialVersionUID = 929872118275916521L;

      ConnectionSetupException(Throwable t)
      {
         super(t);
      }
   }

   /**
    * Special executor used only to work around a MySQL issue that has not been addressed.
    * MySQL issue: http://bugs.mysql.com/bug.php?id=75615
    * 创建同步执行器 实际上没有使用额外的线程执行任务 而是在主线程中同步执行
    */
   private static class SynchronousExecutor implements Executor
   {
      /** {@inheritDoc} */
      @Override
      public void execute(Runnable command)
      {
         try {
            command.run();
         }
         catch (Exception t) {
            LoggerFactory.getLogger(PoolBase.class).debug("Failed to execute: {}", command, t);
         }
      }
   }

   /**
    * 实现类 内部应该是包含一个 统计轨迹对象
    */
   interface IMetricsTrackerDelegate extends AutoCloseable
   {
      default void recordConnectionUsage(PoolEntry poolEntry) {}

      default void recordConnectionCreated(long connectionCreatedMillis) {}

      default void recordBorrowTimeoutStats(long startTime) {}

      default void recordBorrowStats(final PoolEntry poolEntry, final long startTime) {}

      default void recordConnectionTimeout() {}

      @Override
      default void close() {}
   }

   /**
    * A class that delegates to a MetricsTracker implementation.  The use of a delegate
    * allows us to use the NopMetricsTrackerDelegate when metrics are disabled, which in
    * turn allows the JIT to completely optimize away to callsites to record metrics.
    * 代理对象 内部包含 轨迹统计对象
    */
   static class MetricsTrackerDelegate implements IMetricsTrackerDelegate
   {
      final IMetricsTracker tracker;

      MetricsTrackerDelegate(IMetricsTracker tracker)
      {
         this.tracker = tracker;
      }

      @Override
      public void recordConnectionUsage(final PoolEntry poolEntry)
      {
         tracker.recordConnectionUsageMillis(poolEntry.getMillisSinceBorrowed());
      }

      @Override
      public void recordConnectionCreated(long connectionCreatedMillis)
      {
         tracker.recordConnectionCreatedMillis(connectionCreatedMillis);
      }

      @Override
      public void recordBorrowTimeoutStats(long startTime)
      {
         tracker.recordConnectionAcquiredNanos(elapsedNanos(startTime));
      }

      @Override
      public void recordBorrowStats(final PoolEntry poolEntry, final long startTime)
      {
         final long now = currentTime();
         poolEntry.lastBorrowed = now;
         tracker.recordConnectionAcquiredNanos(elapsedNanos(startTime, now));
      }

      @Override
      public void recordConnectionTimeout() {
         tracker.recordConnectionTimeout();
      }

      @Override
      public void close()
      {
         tracker.close();
      }
   }

   /**
    * A no-op implementation of the IMetricsTrackerDelegate that is used when metrics capture is
    * disabled.
    * 不做任何操作的统计对象
    */
   static final class NopMetricsTrackerDelegate implements IMetricsTrackerDelegate {}
}
