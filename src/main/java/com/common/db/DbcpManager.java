/**
 * Purpose: DBCP������
 *
 * @author J.CHEN
 * @version 1.0 2003-12-12
 *
 * Copyright (C) 2003, 2006, KOAL SOFT.
 *
 */

package com.common.db;

import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDriver;
import org.apache.commons.pool.impl.GenericObjectPool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 数据库连接池
 * @version 0.9
 * @since 0.9
 */

public class DbcpManager {

    private static Map<String,Boolean> configured = new HashMap<String,Boolean>();
    
    private static final String POOLDRV_NAME = "jdbc:apache:commons:dbcp:";
    
    public static final GenericObjectPool init(String poolname, String jdbcDriver,
                                               String dbUrl, String dbUser, String dbPwd, String dbCharSet, int maxActive) throws SQLException, ClassNotFoundException {
        synchronized (DbcpManager.class) {
            return startDbcp(poolname, jdbcDriver, dbUrl, dbUser, dbPwd, dbCharSet, maxActive, null);
        }
    }
    
    public static final GenericObjectPool init(String poolname, String jdbcDriver,
            String dbUrl, String dbUser, String dbPwd, String dbCharSet, int maxActive, String validationQuery) throws SQLException, ClassNotFoundException {
        synchronized (DbcpManager.class) {
            return startDbcp(poolname, jdbcDriver, dbUrl, dbUser, dbPwd, dbCharSet, maxActive, validationQuery);
        }
    }


    private static GenericObjectPool startDbcp(String poolname, String jdbcDriver,
         String dbUrl, String dbUser, String dbPwd, String dbCharSet, int maxActive, String validationQuery) throws SQLException, ClassNotFoundException {
        //如果池已经存在，则返回
        if (null != configured.get(poolname)
                && ((Boolean) configured.get(poolname)).booleanValue()) {
            PoolingDriver driver = (PoolingDriver) DriverManager.getDriver(POOLDRV_NAME);
            return (GenericObjectPool) driver.getConnectionPool(poolname);
        }
        
        Class.forName(jdbcDriver);
        
        // construct Pool
        GenericObjectPool connectionPool = new GenericObjectPool(null);
        connectionPool.setTimeBetweenEvictionRunsMillis(300000);
        connectionPool.setMaxWait(10000);
        connectionPool.setMinEvictableIdleTimeMillis(300000);
        connectionPool.setTestOnBorrow(true);
        connectionPool.setMaxActive(maxActive);
        

        DriverManagerConnectionFactory connectionFactory;
        if (dbCharSet == null) {
            connectionFactory = new DriverManagerConnectionFactory(dbUrl,dbUser, dbPwd);
        } else {
            Properties p = new Properties();
            p.setProperty("user", dbUser);
            p.setProperty("password", dbPwd);
            p.setProperty("charset", dbCharSet);
            connectionFactory = new DriverManagerConnectionFactory(dbUrl, p);
        }
            
        new PoolableConnectionFactory(connectionFactory, connectionPool, null, validationQuery, false, true);
        Class.forName("org.apache.commons.dbcp.PoolingDriver");
        PoolingDriver driver = (PoolingDriver) DriverManager.getDriver(POOLDRV_NAME);
        driver.registerPool(poolname, connectionPool);
        if (configured.get(poolname) != null){
            configured.remove(poolname);
        }
        configured.put(poolname, Boolean.TRUE);
        return connectionPool;
    }
    //}}

    /**
     * 根据数据池和数据库名取连接，注意，这里的的数据库别名必须是Global中的变量，如Global.DB_NAME_CA
     * @param poolname 池名称
     * @return
     * @throws SQLException
     */
    public static Connection getConnection(String poolname) throws Exception {
        if (configured.get(poolname) == null) {
            String err = "数据池" + poolname + "未初始化。请先调用初始函数。";
            throw new SQLException(err);
        }
        
        PoolingDriver driver = (PoolingDriver) DriverManager.getDriver(POOLDRV_NAME);
        GenericObjectPool connectionPool = (GenericObjectPool) driver.getConnectionPool(poolname);
        Connection conn = (Connection) connectionPool.borrowObject();
        return conn;        
    }

    /**
     * 清空池
     * @param poolname  池名称
     * @throws SQLException
     */
    public static void clearConnection(String poolname) throws SQLException {
        if (configured.get(poolname) == null) {
            return;
        }
        PoolingDriver driver = (PoolingDriver) DriverManager.getDriver(POOLDRV_NAME);
        GenericObjectPool connectionPool = (GenericObjectPool) driver.getConnectionPool(poolname);
        connectionPool.clear();
    }
}
