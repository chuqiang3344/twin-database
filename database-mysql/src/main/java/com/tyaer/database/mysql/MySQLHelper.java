package com.tyaer.database.mysql;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * mysql操作工具
 * Created by Twin on 2016/6/17.
 */
public abstract class MySQLHelper {

    private static final Logger logger = Logger.getLogger(MySQLHelper.class);
    /**
     * 查询超时时间
     */
    private static final int QUERY_TIMEOUT = 20;
    // 驱动信息
    private static final String DRIVER = "com.mysql.jdbc.Driver";
    // 数据库地址
    protected String url;
    // 数据库用户名
    protected String userName;
    // 数据库密码
    protected String passWord;

    protected MySQLHelper(String USERNAME, String PASSWORD, String URL) {
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
     * 处理语句
     *
     * @param sql
     * @return
     * @throws SQLException
     */
    public int executeSql(String sql) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            int i = pstmt.executeUpdate();
            return i;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeConnect(null, pstmt, conn);
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
            closeConnect(null, pstmt, conn);
        }
        return false;
    }

    public boolean updateByPreparedStatement(String sql, List<Object> params, Connection conn) {
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
            closeConnect(null, pstmt, conn);
        }
        return false;
    }

    /**
     * 批量更新
     *
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
            closeConnect(null, pstmt, conn);
        }
    }

    /**
     * 查询单条记录，多条结果也只返回第一条
     */
    public Map<String, Object> findSimpleResult(String sql, List<Object> params) {
        sql = sql.replace(";", "") + " limit 0,1";
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
            pstmt.setQueryTimeout(QUERY_TIMEOUT);//设置查询超时时间
            resultSet = pstmt.executeQuery();// 返回查询结果
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
            pstmt.setQueryTimeout(QUERY_TIMEOUT);//设置查询超时时间
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
    public <T> T findSimpleRefResult(String sql, List<Object> params, Class<T> cls) {
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
            pstmt.setQueryTimeout(QUERY_TIMEOUT);//设置查询超时时间
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
     *
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
     *
     * @param sql
     * @param limit 每页条数
     * @return
     */
    public List<String> pagingQuery(String table, String sql, int limit) {
        String max_sql = "SELECT COUNT(*) max FROM " + table;
        int max = Integer.valueOf(findSimpleResult(max_sql, null).get("max").toString());
        System.out.println("总数：" + max);
//        int max = list.size();
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

    /**
     * 回收链接 or 关闭连接
     *
     * @param resultSet
     * @param pstmt
     * @param conn
     */
    protected abstract void closeConnect(ResultSet resultSet, PreparedStatement pstmt, Connection conn);

    /**
     * 获取数据库连接
     *
     * @return
     * @throws Exception
     */
    protected abstract Connection getConnection() throws Exception;

}
