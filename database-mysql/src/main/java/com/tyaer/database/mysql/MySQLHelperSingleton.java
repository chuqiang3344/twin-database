package com.tyaer.database.mysql;

import java.sql.*;

/**
 * mysql操作工具
 *
 * @author Twin
 */
public class MySQLHelperSingleton extends MySQLHelper {

    public MySQLHelperSingleton(String USERNAME, String PASSWORD, String URL) {
        super(USERNAME, PASSWORD, URL);
    }

    /**
     * 获得数据库的连接,单连接
     */
    protected Connection getConnection() throws Exception {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url, userName, passWord);
        } catch (Exception e) {
            System.out.println("数据库连接失败！！");
            throw e;
        }
        return connection;
    }

    protected void closeConnect(ResultSet resultSet, PreparedStatement pstmt, Connection conn) {
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
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
