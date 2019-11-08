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

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Enumeration;
import java.util.Map.Entry;
import java.util.Properties;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 这个是 hikari 内置的 数据源
 */
public final class DriverDataSource implements DataSource
{
   private static final Logger LOGGER = LoggerFactory.getLogger(DriverDataSource.class);
   private static final String PASSWORD = "password";
   private static final String USER = "user";

   /**
    * jdbc 驱动地址
    */
   private final String jdbcUrl;
   /**
    * 驱动相关属性
    */
   private final Properties driverProperties;
   /**
    * 驱动对象
    */
   private Driver driver;

   /**
    *
    * @param jdbcUrl   jdbc地址
    * @param driverClassName   驱动类名(从conf中获取)
    * @param properties     dataSource相关属性(从conf.dataSourceProperty中获取)
    * @param username     连接到jdbc的用户名
    * @param password     连接到jdbc的密码
    */
   public DriverDataSource(String jdbcUrl, String driverClassName, Properties properties, String username, String password)
   {
      this.jdbcUrl = jdbcUrl;
      this.driverProperties = new Properties();

      // 这里复制了一份数据
      for (Entry<Object, Object> entry : properties.entrySet()) {
         driverProperties.setProperty(entry.getKey().toString(), entry.getValue().toString());
      }

      // 保存用户名和密码
      if (username != null) {
         driverProperties.put(USER, driverProperties.getProperty("user", username));
      }
      if (password != null) {
         driverProperties.put(PASSWORD, driverProperties.getProperty("password", password));
      }

      if (driverClassName != null) {
         // 通过spi 机制加载 jdbc驱动
         Enumeration<Driver> drivers = DriverManager.getDrivers();
         while (drivers.hasMoreElements()) {
            Driver d = drivers.nextElement();
            // 找到名字匹配的驱动 并设置到该对象中
            if (d.getClass().getName().equals(driverClassName)) {
               driver = d;
               break;
            }
         }

         // 没有找到对应的驱动
         if (driver == null) {
            LOGGER.warn("Registered driver with driverClassName={} was not found, trying direct instantiation.", driverClassName);
            Class<?> driverClass = null;
            ClassLoader threadContextClassLoader = Thread.currentThread().getContextClassLoader();
            try {
               if (threadContextClassLoader != null) {
                  try {
                     // 尝试使用 驱动名直接进行初始化
                     driverClass = threadContextClassLoader.loadClass(driverClassName);
                     LOGGER.debug("Driver class {} found in Thread context class loader {}", driverClassName, threadContextClassLoader);
                  }
                  catch (ClassNotFoundException e) {
                     LOGGER.debug("Driver class {} not found in Thread context class loader {}, trying classloader {}",
                                  driverClassName, threadContextClassLoader, this.getClass().getClassLoader());
                  }
               }

               // 使用当前线程的 类加载器加载失败后尝试使用本对象对应的类加载器加载驱动
               // 什么时候会存在多类加载器的场景呢  可能要关注下tomcat 关于类加载器的处理
               if (driverClass == null) {
                  driverClass = this.getClass().getClassLoader().loadClass(driverClassName);
                  LOGGER.debug("Driver class {} found in the HikariConfig class classloader {}", driverClassName, this.getClass().getClassLoader());
               }
            } catch (ClassNotFoundException e) {
               LOGGER.debug("Failed to load driver class {} from HikariConfig class classloader {}", driverClassName, this.getClass().getClassLoader());
            }

            if (driverClass != null) {
               try {
                  driver = (Driver) driverClass.newInstance();
               } catch (Exception e) {
                  LOGGER.warn("Failed to create instance of driver class {}, trying jdbcUrl resolution", driverClassName, e);
               }
            }
         }
      }

      // 将密码这部分替换成了 特殊的字符
      final String sanitizedUrl = jdbcUrl.replaceAll("([?&;]password=)[^&#;]*(.*)", "$1<masked>$2");
      try {
         // 如果还是没有驱动 尝试使用 jdbcUrl 进行初始化
         if (driver == null) {
            driver = DriverManager.getDriver(jdbcUrl);
            LOGGER.debug("Loaded driver with class name {} for jdbcUrl={}", driver.getClass().getName(), sanitizedUrl);
         }
         // 检验 jdbcUrl 是否合法
         else if (!driver.acceptsURL(jdbcUrl)) {
            throw new RuntimeException("Driver " + driverClassName + " claims to not accept jdbcUrl, " + sanitizedUrl);
         }
      }
      catch (SQLException e) {
         throw new RuntimeException("Failed to get driver instance for jdbcUrl=" + sanitizedUrl, e);
      }
   }

   @Override
   public Connection getConnection() throws SQLException
   {
      return driver.connect(jdbcUrl, driverProperties);
   }

   @Override
   public Connection getConnection(final String username, final String password) throws SQLException
   {
      final Properties cloned = (Properties) driverProperties.clone();
      if (username != null) {
         cloned.put("user", username);
         if (cloned.containsKey("username")) {
            cloned.put("username", username);
         }
      }
      if (password != null) {
         cloned.put("password", password);
      }

      return driver.connect(jdbcUrl, cloned);
   }

   @Override
   public PrintWriter getLogWriter() throws SQLException
   {
      throw new SQLFeatureNotSupportedException();
   }

   @Override
   public void setLogWriter(PrintWriter logWriter) throws SQLException
   {
      throw new SQLFeatureNotSupportedException();
   }

   @Override
   public void setLoginTimeout(int seconds) throws SQLException
   {
      DriverManager.setLoginTimeout(seconds);
   }

   @Override
   public int getLoginTimeout() throws SQLException
   {
      return DriverManager.getLoginTimeout();
   }

   @Override
   public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException
   {
      return driver.getParentLogger();
   }

   @Override
   public <T> T unwrap(Class<T> iface) throws SQLException
   {
      throw new SQLFeatureNotSupportedException();
   }

   @Override
   public boolean isWrapperFor(Class<?> iface) throws SQLException
   {
      return false;
   }
}
