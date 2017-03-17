package com.tyaer.database.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * mysql操作工具
 *
 * @author Twin
 */
public class MySQLHelperPool extends MySQLHelper {
    private static MySQLConnectPool mySQLConnectPool;

    public MySQLHelperPool(String USERNAME, String PASSWORD, String URL) {
        super(USERNAME, PASSWORD, URL);
        mySQLConnectPool = new MySQLConnectPool(URL, USERNAME, PASSWORD);
    }

    /**
     * 获得数据库的连接，连接池
     */
    public Connection getConnection() throws Exception {
        Connection connection = mySQLConnectPool.getConnection();
        return connection;
    }

    protected void closeConnect(ResultSet resultSet, PreparedStatement pstmt,Connection conn) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (pstmt != null) {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            mySQLConnectPool.releaseConnection(conn);
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
