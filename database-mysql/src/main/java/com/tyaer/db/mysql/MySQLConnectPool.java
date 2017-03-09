package com.tyaer.db.mysql;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Twin on 2017/1/17.
 */
public class MySQLConnectPool {
    private static int initPoolSize = 5;
    private static int maxPoolSize = 20;
    private static int waitTime = 100;

    static {
        Properties properties = new Properties();
        try {
            BufferedInputStream inStream = new BufferedInputStream(MySQLConnectPool.class.getResourceAsStream("/mysql.pool.properties"));
            properties.load(inStream);
            initPoolSize = Integer.valueOf(properties.getProperty("mylsql.pool.initPoolSize"));
            maxPoolSize = Integer.valueOf(properties.getProperty("mylsql.pool.maxPoolSize"));
            waitTime = Integer.valueOf(properties.getProperty("get.connect.wait.time"));
            inStream.close();
            properties.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //    private static volatile MySQLConnectPool mySQLConnectPool;
    private long start_time = 0l;
    private MysqlDataSource mysqlDataSource;
    private ConcurrentHashMap<Connection, Boolean> connectionBooleanMap;

    public MySQLConnectPool(String url, String username, String password) {
        start_time = Calendar.getInstance().getTimeInMillis();
        try {
            mysqlDataSource = new MysqlDataSource();
            mysqlDataSource.setUrl(url);
            mysqlDataSource.setUser(username);
            mysqlDataSource.setPassword(password);
            mysqlDataSource.setCacheCallableStmts(true);
            mysqlDataSource.setLoginTimeout(3);//2000 s
            mysqlDataSource.setConnectTimeout(15000);//1000 ms
            mysqlDataSource.setUseUnicode(true);
            mysqlDataSource.setEncoding("UTF-8");
            mysqlDataSource.setZeroDateTimeBehavior("convertToNull");
            mysqlDataSource.setMaxReconnects(3);
            mysqlDataSource.setAutoReconnect(true);
            mysqlDataSource.setAutoReconnectForPools(true);
            mysqlDataSource.setAutoReconnectForConnectionPools(true);

            connectionBooleanMap = new ConcurrentHashMap<>();
            //创建初始化连接
            for (int i = 0; i < initPoolSize; i++) {
                Connection newConnection = getNewConnection();
                if (newConnection != null) {
                    connectionBooleanMap.put(newConnection, true);
                }
            }
            if (connectionBooleanMap.size() == 0) {
                System.out.println("MySQL连接池启动失败，程序中止！");
                System.exit(0);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

//    public static MySQLConnectPool getInstance() {
//        if (mySQLConnectPool == null) {
//            synchronized (MySQLConnectPool.class) {
//                if (mySQLConnectPool == null) {
//                    mySQLConnectPool = new MySQLConnectPool();
//                }
//            }
//        }
//        return mySQLConnectPool;
//    }

    public Connection getNewConnection() {
        try {
            return mysqlDataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public synchronized Connection getConnection() {
        Connection conn = null;
        try {
            for (Entry<Connection, Boolean> entry : connectionBooleanMap.entrySet()) {
                if (entry.getValue()) {
                    conn = entry.getKey();
                    connectionBooleanMap.put(conn, false);
                    break;
                }
            }
            if (conn == null) {
                if (connectionBooleanMap.size() < maxPoolSize) {
                    conn = getNewConnection();
                    connectionBooleanMap.put(conn, false);
                } else {
                    wait(waitTime);
                    conn = getConnection();//递归获取
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    public void releaseConnection(Connection conn) {
        if (conn == null) {
            return;
        }
        try {
            if (connectionBooleanMap.containsKey(conn)) {
                if (conn.isClosed()) {
                    connectionBooleanMap.remove(conn);
                } else {
                    if (!conn.getAutoCommit()) {
                        conn.setAutoCommit(true);
                    }
                    connectionBooleanMap.put(conn, true);
                }
            } else {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void releaseConnectionPool() throws SQLException {
        for (Connection connection : connectionBooleanMap.keySet()) {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
            connectionBooleanMap.remove(connection);
        }
        System.out.println("关闭连接池：" + connectionBooleanMap.size());
    }

    /**
     * 显示连接池当前状态
     */
    public void showStatus() {
        System.out.println("--------------MySQL连接池状态信息--------------");
        System.out.println("运行时间：" + (Calendar.getInstance().getTimeInMillis() - start_time));
        System.out.println("总连接数：" + connectionBooleanMap.size());
        Collection<Boolean> values = connectionBooleanMap.values();
        int i = 0;
        for (Boolean value : values) {
            if (value) {
                i++;
            }
        }
        System.out.println("可用连接数：" + i);
        System.out.println("-----------------------------------------------");
    }

}
