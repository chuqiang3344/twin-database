package com.tyaer.db.mysql;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Twin on 2016/6/17.
 */
public class IDatabase {
    private static Logger logger = Logger.getLogger(IDatabase.class);
    private static Connection CONNECTION;
    // 数据库地址
    private String url;
    // 数据库用户名
    private String userName;
    // 数据库密码
    private String passWord;

    protected IDatabase(String DRIVER, String USERNAME, String PASSWORD, String URL) {
        userName = USERNAME;
        passWord = PASSWORD;
        url = URL;
        try {
            Class.forName(DRIVER);
//            System.out.println("数据库驱动加载完毕！");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获得数据库的连接,单连接
     */
    protected Connection getConnection() throws Exception {
        if (CONNECTION == null || CONNECTION.isClosed()) {
            synchronized (IDatabase.class) {
                if (CONNECTION == null || CONNECTION.isClosed()) {
                    try {
                        CONNECTION = DriverManager.getConnection(url, userName, passWord);
                    } catch (Exception e) {
                        System.out.println("数据库连接失败！！");
                        throw e;
//            e.printStackTrace();
                    }
                }
            }
        }
        return CONNECTION;
    }

    /**
     * 处理语句
     * @param sql
     * @return
     * @throws SQLException
     */
    public int executeSql(String sql) throws SQLException {
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            int i = pstmt.executeUpdate();
            return i;
        } catch (Exception e) {
//            e.printStackTrace();
            System.out.println(ExceptionUtils.getStackFrames(e));
        } finally {
            closeConnect(pstmt, conn);
        }
        return 0;
    }

    /**
     * replace语句，更新一条数据
     *
     * @param sql
     * @param obj
     */
    public void replace(String sql, Object obj) {
        Class<?> objClass = obj.getClass();
        Field[] fields = objClass.getDeclaredFields();
        ArrayList<Object> grams = new ArrayList<Object>();
        for (int i = 0; i < fields.length; i++) {
            String name = fields[i].getName();
            try {
                Field f = objClass.getDeclaredField(name);
                f.setAccessible(true);
                Object value = f.get(obj);
                grams.add(value);
            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
//        System.out.println(grams);
        updateByPreparedStatement(sql, grams);
    }

    /**
     * 增加、删除、改
     */
    public boolean updateByPreparedStatement(String sql, List<Object> params) {
        boolean flag;
        int result;
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            int index = 1;
            // 传入查询参数
            if (params != null && !params.isEmpty()) {
                for (Object param : params) {
                    pstmt.setObject(index++, param);
                }
            }
            result = pstmt.executeUpdate();
            // 更新成功result>0,更新失败result=0，操作数据条数2=删除、增加
//			System.out.println(result);
            flag = result > 0;
            return flag;
        } catch (Exception e) {
//            logger.error(ExceptionUtils.getMessage(e));
            e.printStackTrace();
        } finally {
            closeConnect(pstmt, conn);
        }
        return false;
    }

    public boolean updateByPreparedStatement(String sql, List<Object> params,Connection conn) {
        boolean flag;
        int result;
        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement(sql);
            int index = 1;
            // 传入查询参数
            if (params != null && !params.isEmpty()) {
                for (int i = 0; i < params.size(); i++) {
                    pstmt.setObject(index++, params.get(i));
                }
            }
            result = pstmt.executeUpdate();
            // 更新成功result>0,更新失败result=0
            flag = result > 0 ? true : false;
            return flag;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeConnect(pstmt, conn);
        }
        return false;
    }

    /**
     * 批量更新
     * @param sql
     * @param paramsList
     */
    public void batchUpdateByPreparedStatement(String sql, ArrayList<ArrayList<Object>> paramsList) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = getConnection();
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            for (List<Object> params : paramsList) {
                int index = 1;
                // 传入查询参数
                if (params != null && !params.isEmpty()) {
                    for (Object param : params) {
                        pstmt.setObject(index++, param);
                    }
                    pstmt.addBatch();
                }
            }
            pstmt.executeBatch();
            conn.commit();
        } catch (Exception e) {
            logger.error(ExceptionUtils.getMessage(e));
        } finally {
            closeConnect(pstmt, conn);
        }
    }

    /**
     * 查询单条记录
     */
    public Map<String, Object> findSimpleResult(String sql, List<Object> params) {
        Map<String, Object> resultSetMap = new HashMap<String, Object>();
        int index = 1;
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            // 传入查询参数
            if (params != null && !params.isEmpty()) {
                for (int i = 0; i < params.size(); i++) {
                    pstmt.setObject(index++, params.get(i));
                }
            }
            pstmt.setQueryTimeout(15);//设置查询超时时间
            resultSet = pstmt.executeQuery();// 返回查询结果
            // TODO: 2017/1/22  多条结果也只返回第一条
            if (resultSet.next()) {
                resultSetMap = getResultSetMap(resultSet);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeConnect(resultSet, pstmt, conn);
        }
        return resultSetMap;
    }

    /**
     * 查询多条记录
     */
    public List<Map<String, Object>> findModeResult(String sql, List<Object> params) {
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        int index = 1;
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            // 传入查询参数
            if (params != null && !params.isEmpty()) {
                for (Object param : params) {
                    pstmt.setObject(index++, param);
                }
            }
            pstmt.setQueryTimeout(15);//设置查询超时时间
            resultSet = pstmt.executeQuery();// 返回查询结果,阻塞
            while (resultSet.next()) {
                Map<String, Object> resultSetMap = getResultSetMap(resultSet);
                list.add(resultSetMap);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(ExceptionUtils.getMessage(e));
        } finally {
            closeConnect(resultSet, pstmt, conn);
        }
        return list;
    }

    /**
     * 通过反射机制查询单条记录
     */
    public <T> T findSimpleRefResult(String sql, List<Object> params,Class<T> cls) {
        T resultObject = null;
        int index = 1;
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            // 传入查询参数
            if (params != null && !params.isEmpty()) {
                for (int i = 0; i < params.size(); i++) {
                    pstmt.setObject(index++, params.get(i));
                }
            }
            pstmt.setQueryTimeout(15);//设置查询超时时间
            resultSet = pstmt.executeQuery();
            if (resultSet.next()) {
                //第一条
                resultObject = getSimpleResult(cls, resultSet);
            }
            return resultObject;
        } catch (Exception e) {
            logger.error(ExceptionUtils.getStackTrace(e));
        } finally {
            closeConnect(resultSet, pstmt, conn);
        }
        return null;
    }

    /**
     * 通过反射机制查询多条记录
     */
    public <T> List<T> findMoreRefResult(String sql, List<Object> params, Class<T> cls) {
        List<T> list = new ArrayList<T>();
        int index = 1;
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            if (params != null && !params.isEmpty()) {
                for (int i = 0; i < params.size(); i++) {
                    pstmt.setObject(index++, params.get(i));
                }
            }
            resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                // 通过反射机制创建一个实例
                T resultObject = getSimpleResult(cls, resultSet);
                list.add(resultObject);
            }
            return list;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeConnect(resultSet, pstmt, conn);
        }
        return null;
    }

    private <T> T getSimpleResult(Class<T> cls, ResultSet resultSet) throws InstantiationException, IllegalAccessException, SQLException, NoSuchFieldException {
        T resultObject = cls.newInstance();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int cols_len = metaData.getColumnCount();
        for (int i = 0; i < cols_len; i++) {
            String cols_name = metaData.getColumnName(i + 1);
            Object cols_value = resultSet.getObject(cols_name);
//            if (cols_value == null) {
//                cols_value = "";
//            }
            try {
                Field field = cls.getDeclaredField(cols_name);
                field.setAccessible(true); // 打开javabean的访问权限
                field.set(resultObject, cols_value);
            } catch (NoSuchFieldException e) {
                System.out.println("bean have not this field: " + cols_name);
            }
        }
        return resultObject;
    }

    /**
     * 类转换为map
     * @param resultSet
     * @return
     * @throws SQLException
     */
    private Map<String, Object> getResultSetMap(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();// 得到列名集合
        int col_len = metaData.getColumnCount();// 得到列名个数
        Map<String, Object> map = new HashMap<String, Object>();
        for (int i = 0; i < col_len; i++) {
            String cols_name = metaData.getColumnName(i + 1);
            // 得到属性值
            Object cols_value = resultSet.getObject(cols_name);
            // 如果属性值为null则设置为空字符串
            if (cols_value == null) {
                cols_value = "";
            }
            map.put(cols_name, cols_value);
        }
        return map;
    }

    /**
     * 分页查询sql
     */
    public List<String> pagingQuery(String sql, int limit) {
        List<Map<String, Object>> list = findModeResult(sql, null);
        int max = list.size();
        int start = 0;
        int pageNum = 0;
        if (max % limit > 0) {
            pageNum = max / limit + 1;
        } else {
            pageNum = max / limit;
        }
        List<String> paginglist = new ArrayList<String>();
        String limitSql = null;
        for (int i = 0; i < pageNum; i++) {
            limitSql = sql + " LIMIT " + start + "," + limit;
            paginglist.add(limitSql);
            start = start + limit;
        }
        return paginglist;
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
//            if (conn != null) {
//                conn.close();
//            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    protected void closeConnect(PreparedStatement pstmt, Connection conn) {
        try {
            if (pstmt != null) {
                pstmt.close();
            }
//            if (conn != null) {
//                conn.close();
//            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

//    private void closeConnect() {
//        try {
//            if (CONNECTION != null) {
//                CONNECTION.close();
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//    }

    public void showQueryResult(List<Map<String, Object>> list) {
        for (Object obj : list) {
            System.out.println(obj.toString());
        }
    }

}
