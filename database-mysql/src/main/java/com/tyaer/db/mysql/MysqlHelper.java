package com.tyaer.db.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * mysql操作工具
 *
 * @author Twin
 */
public class MysqlHelper extends IDatabase {
    // 驱动信息
    private static final String DRIVER = "com.mysql.jdbc.Driver";
    private static MySQLConnectPool mySQLConnectPool;

    public MysqlHelper(String USERNAME, String PASSWORD, String URL) {
        super(DRIVER, USERNAME, PASSWORD, URL);
        mySQLConnectPool = new MySQLConnectPool(URL, USERNAME, PASSWORD);
    }

    /**
     * 获得数据库的连接，连接池
     */
    public Connection getConnection() throws Exception {
        Connection connection = mySQLConnectPool.getConnection();
        return connection;
    }

    protected void closeConnect(ResultSet resultSet, PreparedStatement pstmt,
                                Connection conn) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (pstmt != null) {
                pstmt.close();
            }
            if (conn != null) {
                mySQLConnectPool.releaseConnection(conn);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    protected void closeConnect(PreparedStatement pstmt, Connection conn) {
        try {
            if (pstmt != null) {
                pstmt.close();
            }
            if (conn != null) {
                mySQLConnectPool.releaseConnection(conn);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void showPoolStatus() {
        mySQLConnectPool.showStatus();
    }

    public void releaseConnectionPool() {
        try {
            mySQLConnectPool.releaseConnectionPool();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
