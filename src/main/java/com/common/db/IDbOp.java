package com.common.db;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

public interface IDbOp {

    /**
     * 缺省的返回记录最大个数，未指定时使用
     */
    public static final int DEFAULT_MAX = Integer.MAX_VALUE;

    /**
     * 插入记录（插入前判断是否存在，如果存在抛出异常）
     * @param conn 数据库连接
     * @param table 表名
     * @param dataMap 插入数据的Map，其中的key就是字段名，value是对应该字段的数据
     * @param condition 判断条件
     * @param condValues 条件参数
     * @throws Exception
     */
    public int insertIfNotExists(Connection conn, String table, Map<String,Object> dataMap, String condition, List<Object> condValues) throws Exception;

    /**
     * 插入记录
     * @param conn 数据库连接
     * @param table 表名
     * @param dataMap 插入数据的Map，其中的key就是字段名，value是对应该字段的数据
     * @param isAutoGenKey 是否返回自增加键值
     * @return 自增加字段的值
     * @throws Exception
     */
    public int insert(Connection conn, String table, Map<String,Object> dataMap, boolean isAutoGenKey) throws Exception;

    /**
     * 插入记录
     * @param conn 数据库连接
     * @param table 表名
     * @param dataMap 插入数据的Map，其中的key就是字段名，value是对应该字段的数据
     * @param colList 插入完成后需要返回的字段名称
     * @param isAutoGenKey 是否返回自增加键值
     * @return 自增加字段的值
     * @throws Exception
     */
    public int insert(Connection conn, String table, Map<String,Object> dataMap, String[] colList, boolean isAutoGenKey) throws Exception;


    /**
     * 插入记录
     * @param conn 数据库连接
     * @param table 表名
     * @param dataMap 插入数据的Map，其中的key就是字段名，value是对应该字段的数据
     * @throws Exception
     */
    public int insert(Connection conn, String table, Map<String,Object> dataMap) throws Exception;

    /**
     * 更新表记录的多个字段
     * @param conn 数据库连接
     * @param table 表名
     * @param dataMap 待更新数据的Map，其中的key就是字段名，value是对应该字段的数据
     * @param condition 条件语句
     * @param condValues 条件参数列表
     * @throws Exception
     */
    public void update(Connection conn, String table, Map<String,Object> dataMap, String condition, List<Object> condValues) throws Exception;

    /**
     * 更新表记录的一个字段
     * @param conn 数据库连接
     * @param table 表名
     * @param name 待更新字段名
     * @param value 待更新数据
     * @param condition 无参数的条件语句
     * @throws Exception
     */
    public void update(Connection conn, String table, String name, Object value, String condition) throws Exception;

    /**
     * 删除记录
     * @param conn 数据库连接
     * @param table 表名
     * @param preparedCond 条件语句
     * @param condValues 条件参数列表
     * @throws Exception
     */
    public void delete(Connection conn, String table, String preparedCond, List<Object> condValues) throws Exception;

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
    public Map<String,Object> getOneRowAsMap(Connection conn, String table, List<String> cols, String condition, List<Object> condValues,
                                             boolean mustUnique) throws Exception;

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
    public Object[] getOneRowAsArray(Connection conn, String table, List<String> cols, String condition, List<Object> condValues,
                                     boolean mustUnique) throws Exception;

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
    public List<Map<String,Object>> searchAsMapList(Connection conn, String table, List<String> cols, String condition, List<Object> condValues, int start,
                                                    int max, String orderBy, String groupBy, boolean bAsc) throws Exception;

    /**
     * 添加一个以数据库Label为key的方法,考虑修改会影响其他人所以另写在一个方法,希望在项目空窗期合并起来
     */
    public List<Map<String,Object>> searchAsMapListInLabel(Connection conn, String table, List<String> cols, String condition, List<Object> condValues, int start,
                                                           int max, String orderBy, String groupBy, boolean bAsc) throws Exception;
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
    public List<Object[]> searchAsArrayList(Connection conn, String table, List<String> cols, String condition, List<Object> condValues,
                                            int start, int max, String orderBy, String groupBy, boolean bAsc) throws Exception;


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
    public int getCount(Connection conn, String table, String condition, List<Object> condValues, String distinct)
            throws Exception;

    /**
     * 得到指定列中的最大值
     * @param type 返回类型
     * @param conn
     * @param table
     * @param col
     * @param condition
     * @param condValues
     * @return
     * @throws Exception
     */
    public <T> T getMax(Class<T> type , Connection conn, String table, String col, String condition, List<Object> condValues)
            throws Exception;

    public <T> T getMin(Class<Integer> type, Connection conn, String table, String colum, String contidion, List<Object> condValues) throws Exception;

    /**
     * 执行sql语句（批量）
     *
     * @param conn
     * @param sqls
     * @throws Exception
     */
    public void execSQL(Connection conn, String[] sqls) throws Exception;
}
