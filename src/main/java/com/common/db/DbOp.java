package com.common.db;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @Author wul
 * @Description
 * @Date 2019/12/11 14:34
 */
public class DbOp {


    /**
     * 默认的最大记录个数，即IDbOp.DEFAULT_MAX
     */
    public static final int DEFAULT_MAX = IDbOp.DEFAULT_MAX;

    /**
     * 内部实际调用的IDbOp实现类，缺省是无优化的通用实现
     */
    private static IDbOp m_dbOp = new DbOpBase();

    /**
     * 启用优化功能，如果优化对象存在，内部实现类切换为特定数据库的IDbOp实现，否则仍是无优化的通用实现
     *
     * @param jdbcDriver jdbc驱动的名称
     * @param version jdbc驱动的版本
     */
    public static void enableOptimize(String jdbcDriver, int version) {
        IDbOp newDbOp = DbOpMgr.getDbOp(jdbcDriver, version);
        if (newDbOp != null){
            m_dbOp = newDbOp;
        }
    }

    /**
     * 插入记录
     * @param conn 数据库连接
     * @param table 表名
     * @param dataMap 插入数据的Map，其中的key就是字段名，value是对应该字段的数据
     * @throws Exception
     */
    public static void insert(Connection conn, String table, Map<String,Object> dataMap) throws Exception {
        m_dbOp.insert(conn, table, dataMap);
    }

    /**
     * 插入记录
     * @param conn 数据库连接
     * @param table 表名
     * @param dataMap 插入数据的Map，其中的key就是字段名，value是对应该字段的数据
     * @throws Exception
     */
    public static int insert(Connection conn, String table, Map<String,Object> dataMap, boolean isAutoGenKey) throws Exception {
        return m_dbOp.insert(conn, table, dataMap, isAutoGenKey);
    }

    /**
     * 更新表记录的多个字段
     * @param conn 数据库连接
     * @param table 表名
     * @param dataMap 待更新数据的Map，其中的key就是字段名，value是对应该字段的数据
     * @param condition 无参数的条件语句
     * @throws Exception
     */
    public static void update(Connection conn, String table, Map<String,Object> dataMap, String condition) throws Exception {
        m_dbOp.update(conn, table, dataMap, condition, null);
    }

    /**
     * 更新表记录的多个字段
     * @param conn 数据库连接
     * @param table 表名
     * @param dataMap 待更新数据的Map，其中的key就是字段名，value是对应该字段的数据
     * @param condition 条件语句
     * @param condValues 条件参数列表
     * @throws Exception
     */
    public static void update(Connection conn, String table, Map<String,Object> dataMap, String condition, List<Object> condValues)
            throws Exception {
        m_dbOp.update(conn, table, dataMap, condition, condValues);
    }

    /**
     * 更新表记录的一个字段
     * @param conn 数据库连接
     * @param table 表名
     * @param name 待更新字段名
     * @param value 待更新数据
     * @param condition 无参数的条件语句
     * @throws Exception
     */
    public static void update(Connection conn, String table, String name, Object value, String condition)
            throws Exception {
        m_dbOp.update(conn, table, name, value, condition);
    }

    /**
     * 删除记录
     * @param conn 数据库连接
     * @param table 表名
     * @param condition 无参数的条件语句
     * @throws Exception
     */
    public static void delete(Connection conn, String table, String condition) throws Exception {
        delete(conn, table, condition, null);
    }

    /**
     * 删除记录
     * @param conn 数据库连接
     * @param table 表名
     * @param condition 条件语句
     * @param condValues 条件参数列表
     * @throws Exception
     */
    public static void delete(Connection conn, String table, String condition, List<Object> condValues) throws Exception {
        m_dbOp.delete(conn, table, condition, condValues);
    }

    /**
     * 获取一条记录，以map返回
     * @param conn 数据库连接
     * @param table 表名
     * @param cols 需要返回的字段名列表
     * @param condition 条件语句
     * @param mustUnique 是否必须唯一？若为True，但实际又有多条记录，将抛出异常
     * @return 数据map
     * @throws Exception
     */
    public static Map<String,Object> getOneRowAsMap(Connection conn, String table, List<String> cols, String condition, boolean mustUnique)
            throws Exception {
        return m_dbOp.getOneRowAsMap(conn, table, cols, condition, null, mustUnique);
    }

    /**
     * 获取一条记录，以map返回
     * @param conn 数据库连接
     * @param table 表名
     * @param cols 需要返回的字段名列表
     * @param condition 条件语句
     * @param condValues 条件参数列表
     * @param mustUnique 是否必须唯一？若为True，但实际又有多条记录，将抛出异常
     * @return 数据map
     * @throws Exception
     */
    public static Map<String,Object> getOneRowAsMap(Connection conn, String table, List<String> cols, String condition, List<Object> condValues,
                                                    boolean mustUnique) throws Exception {
        return m_dbOp.getOneRowAsMap(conn, table, cols, condition, condValues, mustUnique);
    }

    /**
     * 获取一条记录，以数组方式返回。
     * @param conn 数据库连接
     * @param table 表名
     * @param cols 需要返回的字段名列表
     * @param condition 无参数的条件语句
     * @param mustUnique 是否必须唯一？若为True，但实际又有多条记录，将抛出异常
     * @return 数据数组，顺序对应于cols字段顺序
     * @throws Exception
     */
    public static Object[] getOneRowAsArray(Connection conn, String table, List<String> cols, String condition,
                                            boolean mustUnique) throws Exception {
        return m_dbOp.getOneRowAsArray(conn, table, cols, condition, null, mustUnique);
    }

    /**
     * 获取一条记录，以数组方式返回。
     * @param conn 数据库连接
     * @param table 表名
     * @param cols 需要返回的字段名列表
     * @param condition 条件语句
     * @param condValues 条件参数列表
     * @param mustUnique 是否必须唯一？若为True，但实际又有多条记录，将抛出异常
     * @return
     * @throws Exception
     */
    public static Object[] getOneRowAsArray(Connection conn, String table, List<String> cols, String condition,
                                            List<Object> condValues, boolean mustUnique) throws Exception {
        return m_dbOp.getOneRowAsArray(conn, table, cols, condition, condValues, mustUnique);
    }

    /**
     * 通用查询，以List[map1, map2...]形式返回
     * @param conn 数据库连接
     * @param table 表名
     * @return 以List[map1, map2...]形式返回，所有字段数据
     * @throws Exception
     */
    public static List<Map<String,Object>> searchAsMapList(Connection conn, String table) throws Exception {
        return m_dbOp.searchAsMapList(conn, table, null, null, null, 0, DEFAULT_MAX, null, null, false);
    }

    /**
     * 通用查询，以List[map1, map2...]形式返回
     * @param conn 数据库连接
     * @param table 表名
     * @param cols 需要返回的字段名列表
     * @return 以List[map1, map2...]形式返回，cols指定的字段数据
     * @throws Exception
     */
    public static List<Map<String,Object>> searchAsMapList(Connection conn, String table, List<String> cols) throws Exception {
        return m_dbOp.searchAsMapList(conn, table, cols, null, null, 0, DEFAULT_MAX, null, null, false);
    }

    /**
     * 通用查询，以List[map1, map2...]形式返回
     * @param conn 数据库连接
     * @param table 表名
     * @param cols 需要返回的字段名列表
     * @param condition 无参数的条件语句
     * @return 以List[map1, map2...]形式返回，cols指定的字段数据
     * @throws Exception
     */
    public static List<Map<String,Object>> searchAsMapList(Connection conn, String table, List<String> cols, String condition) throws Exception {
        return m_dbOp.searchAsMapList(conn, table, cols, condition, null, 0, DEFAULT_MAX, null, null, false);
    }

    /**
     * 通用查询，以List[map1, map2...]形式返回
     * @param conn 数据库连接
     * @param table 表名
     * @param cols 需要返回的字段名列表
     * @param condition 无参数的条件语句
     * @param orderBy 排序字段名
     * @param bAsc 是否升序
     * @return 以List[map1, map2...]形式返回，cols指定的字段数据
     * @throws Exception
     */
    public static List<Map<String,Object>> searchAsMapList(Connection conn, String table, List<String> cols, String condition, String orderBy,
                                                           boolean bAsc) throws Exception {
        return m_dbOp.searchAsMapList(conn, table, cols, condition, null, 0, DEFAULT_MAX, orderBy, null, bAsc);
    }

    /**
     * 通用查询，以List[map1, map2...]形式返回
     * @param conn 数据库连接
     * @param table 表名
     * @param cols 需要返回的字段名列表
     * @param condition 无参数的条件语句
     * @param start 记录的开始位置
     * @param max 返回最大记录个数
     * @param orderBy 排序字段名
     * @param bAsc 是否升序
     * @return 以List[map1, map2...]形式返回，cols指定的字段数据，最大为max个
     * @throws Exception
     */
    public static List<Map<String,Object>> searchAsMapList(Connection conn, String table, List<String> cols, String condition, int start, int max,
                                                           String orderBy, boolean bAsc) throws Exception {
        return m_dbOp.searchAsMapList(conn, table, cols, condition, null, start, max, orderBy, null, bAsc);
    }

    /**
     * 通用查询，以List[map1, map2...]形式返回
     * @param conn 数据库连接
     * @param table 表名
     * @param cols 需要返回的字段名列表
     * @param condition 条件语句
     * @param condValues 条件参数列表
     * @param start 记录的开始位置
     * @param max 返回最大记录个数
     * @param orderBy 排序字段名
     * @param bAsc 是否升序
     * @return 以List[map1, map2...]形式返回，cols指定的字段数据，最大为max个
     * @throws Exception
     */
    public static List<Map<String,Object>> searchAsMapList(Connection conn, String table, List<String> cols, String condition, List<Object> condValues,
                                                           int start, int max, String orderBy, boolean bAsc) throws Exception {
        return m_dbOp.searchAsMapList(conn, table, cols, condition, condValues, start, max, orderBy, null, bAsc);
    }

    /**
     * 通用查询，以List[map1, map2...]形式返回
     * @param conn 数据库连接
     * @param table 表名
     * @param cols 需要返回的字段名列表
     * @param condition 条件语句
     * @param condValues 条件参数列表
     * @param start 记录的开始位置
     * @param max 返回最大记录个数
     * @param orderBy 排序字段名
     * @param groupBy 分组字段名
     * @param bAsc 是否升序
     * @return 以List[map1, map2...]形式返回，cols指定的字段数据，最大为max个
     * @throws Exception
     */
    public static List<Map<String,Object>> searchAsMapList(Connection conn, String table, List<String> cols, String condition, List<Object> condValues,
                                                           int start, int max, String orderBy, String groupBy, boolean bAsc) throws Exception {
        return m_dbOp.searchAsMapList(conn, table, cols, condition, condValues, start, max, orderBy, groupBy, bAsc);
    }

    /**
     * 通用查询，以List[array1, array2...]形式返回
     * @param conn 数据库连接
     * @param table 表名
     * @return 以List[array1, array2...]形式返回，所有字段数据
     * @throws Exception
     */
    public static List<Object[]> searchAsArrayList(Connection conn, String table) throws Exception {
        return m_dbOp.searchAsArrayList(conn, table, null, null, null, 0, DEFAULT_MAX, null, null, false);
    }

    /**
     * 通用查询，以List[array1, array2...]形式返回
     * @param conn 数据库连接
     * @param table 表名
     * @param cols 需要返回的字段名列表
     * @return 以List[array1, array2...]形式返回，cols指定的字段数据
     * @throws Exception
     */
    public static List<Object[]> searchAsArrayList(Connection conn, String table, List<String> cols) throws Exception {
        return m_dbOp.searchAsArrayList(conn, table, cols, null, null, 0, DEFAULT_MAX, null, null, false);
    }

    /**
     * 通用查询，以List[array1, array2...]形式返回
     * @param conn 数据库连接
     * @param table 表名
     * @param cols 需要返回的字段名列表
     * @param condition 无参数的条件语句
     * @return 以List[array1, array2...]形式返回，cols指定的字段数据
     * @throws Exception
     */
    public static List<Object[]> searchAsArrayList(Connection conn, String table, List<String> cols, String condition) throws Exception {
        return m_dbOp.searchAsArrayList(conn, table, cols, condition, null, 0, DEFAULT_MAX, null, null, false);
    }

    /**
     * 通用查询，以List[array1, array2...]形式返回
     * @param conn 数据库连接
     * @param table 表名
     * @param cols 需要返回的字段名列表
     * @param condition 无参数的条件语句
     * @param orderBy 排序字段名
     * @param bAsc 是否升序
     * @return 以List[array1, array2...]形式返回，cols指定的字段数据
     * @throws Exception
     */
    public static List<Object[]> searchAsArrayList(Connection conn, String table, List<String> cols, String condition, String orderBy,
                                                   boolean bAsc) throws Exception {
        return m_dbOp.searchAsArrayList(conn, table, cols, condition, null, 0, DEFAULT_MAX, orderBy, null, bAsc);
    }

    /**
     * 通用查询，以List[array1, array2...]形式返回
     * @param conn 数据库连接
     * @param table 表名
     * @param cols 需要返回的字段名列表
     * @param condition 无参数的条件语句
     * @param start 记录的开始位置
     * @param max 返回最大记录个数
     * @param orderBy 排序字段名
     * @param bAsc 是否升序
     * @return 以List[array1, array2...]形式返回，cols指定的字段数据
     * @throws Exception
     */
    public static List<Object[]> searchAsArrayList(Connection conn, String table, List<String> cols, String condition, int start,
                                                   int max, String orderBy, boolean bAsc) throws Exception {
        return m_dbOp.searchAsArrayList(conn, table, cols, condition, null, start, max, orderBy, null, bAsc);
    }

    /**
     * 通用查询，以List[array1, array2...]形式返回
     * @param conn 数据库连接
     * @param table 表名
     * @param cols 需要返回的字段名列表
     * @param condition 条件语句
     * @param condValues 条件参数列表
     * @param start 记录的开始位置
     * @param max 返回最大记录个数
     * @param orderBy 排序字段名
     * @param bAsc 是否升序
     * @return 以List[array1, array2...]形式返回，cols指定的字段数据
     * @throws Exception
     */
    public static List<Object[]> searchAsArrayList(Connection conn, String table, List<String> cols, String condition, List<Object> condValues,
                                                   int start, int max, String orderBy, boolean bAsc) throws Exception {
        return m_dbOp.searchAsArrayList(conn, table, cols, condition, condValues, start, max, orderBy, null, bAsc);
    }

    /**
     * 通用查询，以List[array1, array2...]形式返回
     * @param conn 数据库连接
     * @param table 表名
     * @param cols 需要返回的字段名列表
     * @param condition 条件语句
     * @param condValues 条件参数列表
     * @param start 记录的开始位置
     * @param max 返回最大记录个数
     * @param orderBy 排序字段名
     * @param groupBy 分组字段名
     * @param bAsc 是否升序
     * @return 以List[array1, array2...]形式返回，cols指定的字段数据
     * @throws Exception
     */
    public static List<Object[]> searchAsArrayList(Connection conn, String table, List<String> cols, String condition, List<Object> condValues,
                                                   int start, int max, String orderBy, String groupBy, boolean bAsc) throws Exception {
        return m_dbOp.searchAsArrayList(conn, table, cols, condition, condValues, start, max, orderBy, groupBy, bAsc);
    }

    /**
     * 得到记录个数
     * @param conn 数据库连接
     * @param table 表名
     * @param condition 条件语句
     * @param condValues 条件语句列表
     * @param distinct 数据库distinct
     * @return 符合条件的记录个数
     * @throws Exception
     */
    public static int getCount(Connection conn, String table, String condition, List<Object> condValues, String distinct)
            throws Exception {
        return m_dbOp.getCount(conn, table, condition, condValues, distinct);
    }

}
