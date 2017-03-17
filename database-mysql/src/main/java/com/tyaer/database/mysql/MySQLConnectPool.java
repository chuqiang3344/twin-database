package com.tyaer.database.mysql;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.apache.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by Twin on 2017/1/17.
 */
public class MySQLConnectPool {
    private static final Logger logger = Logger.getLogger(MySQLConnectPool.class);
    private static final long MARK_USING = 0L;
    private static int initPoolSize = 1;
    private static int maxPoolSize = 5;
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
    private ConcurrentHashMap<Connection, Long> connectionBooleanMap;
    private ScheduledExecutorService scheduledExecutorService;

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
                    connectionBooleanMap.put(newConnection, getCurrentTime());
                } else {
                    logger.warn("创建新连接失败！");
                }
            }
            if (connectionBooleanMap.size() == 0) {
                logger.warn("MySQL连接池启动失败，程序中止！");
                System.exit(0);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        //启动回收线程
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(new Warder(), 1, 5, TimeUnit.MINUTES);
    }

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
            for (Entry<Connection, Long> entry : connectionBooleanMap.entrySet()) {
                if (isAvailable(entry.getValue())) {
                    conn = entry.getKey();
                    connectionBooleanMap.put(conn, MARK_USING);
                    break;
                }
            }
            if (conn == null) {
                if (connectionBooleanMap.size() < maxPoolSize) {
                    conn = getNewConnection();
                    connectionBooleanMap.put(conn, MARK_USING);
                } else {
                    wait(waitTime);
                    conn = getConnection();// TODO: 2017/3/15  递归获取
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
                    connectionBooleanMap.put(conn, getCurrentTime());
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
        connectionBooleanMap.clear();
        scheduledExecutorService.shutdown();
        logger.info("释放连接池：" + connectionBooleanMap.size());
    }

    /**
     * 显示连接池当前状态
     */
    public void showStatus() {
        logger.info("--------------MySQL连接池状态信息--------------");
        logger.info("运行时间：" + (getCurrentTime() - start_time));
        logger.info("总连接数：" + connectionBooleanMap.size());
        Collection<Long> values = connectionBooleanMap.values();
        int i = 0;
        for (Long value : values) {
            if (isAvailable(value)) {
                i++;
            }
        }
        logger.info("可用连接数：" + i);
        logger.info("-----------------------------------------------");
    }

    private boolean isAvailable(Long value) {
        return value == MARK_USING ? false : true;
    }

    /**
     * 标记正在使用状态
     *
     * @return
     */
    private long getCurrentTime() {
        return Calendar.getInstance().getTimeInMillis();
    }

    /**
     * 自动关闭超时连接
     */
    class Warder extends Thread {
        private final Long OVERTIME_TIME = 10 * 60 * 1000L;
//        private final Long OVERTIME_TIME = 5 * 1000L;

        @Override
        public void run() {
            showStatus();
            Set<Entry<Connection, Long>> entries = connectionBooleanMap.entrySet();
            long time = getCurrentTime();
            for (Entry<Connection, Long> entry : entries) {
                Long value = entry.getValue();
                if (value != MARK_USING && time - value > OVERTIME_TIME) {
                    Connection connection = entry.getKey();
                    try {
                        if (connection != null && !connection.isClosed()) {
                            logger.info("释放空闲超时连接..." + connectionBooleanMap.size());
                            connection.close();
                        }
                        connectionBooleanMap.remove(connection);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
