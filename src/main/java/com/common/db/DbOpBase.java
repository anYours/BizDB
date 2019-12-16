package com.common.db;

import com.common.util.EmptyUtils;
import com.common.util.SQLUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;

/**
 * @Author wul
 * @Description
 * @Date 2019/12/11 11:18
 */
@Slf4j
public class DbOpBase implements IDbOp, Serializable {

    private static final long serialVersionUID = 1L;
    /**
     * 根据数值的类型调用不同的setXXX方法
     *
     * @param pstmt
     * @param values
     * @throws Exception
     */
    protected static final void preparedStatementSetValues(
            PreparedStatement pstmt, List<Object> values) throws Exception {
        for (int i = 0; i < values.size(); i++) {
            if (values.get(i) instanceof java.util.Date) {
                java.util.Date date = (java.util.Date) values.get(i);
                pstmt.setTimestamp(i + 1, new Timestamp(date.getTime()));
            } else {
                pstmt.setObject(i + 1, values.get(i));
            }
        }

    }

    /**
     * 返回ResultSet结果中的数据，会根据数据库类型自动转换为java类型
     *
     * @param index
     * @param rs
     * @param rsmd
     * @return
     * @throws Exception
     */
    protected static final Object getRsObj(int index, ResultSet rs,
                                           ResultSetMetaData rsmd) throws Exception {
        switch (rsmd.getColumnType(index)) {
            case Types.LONGVARBINARY:
            case Types.VARBINARY:
            case Types.BINARY:
            case Types.BLOB:
                byte[] data = rs.getBytes(index);
                if (data != null) {
                    return new String(data);
                } else {
                    return null;
                }
            case Types.CLOB:
                Clob clob = rs.getClob(index);
                if (clob != null) {
                    return clob
                            .getSubString(1, Integer.valueOf(clob.length() + ""));
                } else {
                    return null;
                }
            case Types.NUMERIC:
                // oracle 没有 boolean的专用sqlType, 以number(1)代替 by maj 2016.11.02
                switch (rsmd.getPrecision(index)) {
                    case 1:
                        // 仅为oracle的情况下使用 by maj 2016.11.02
                        if (rsmd.getClass().getPackage().getName()
                                .equals("oracle.jdbc.driver")) {
                            Boolean b = rs.getBoolean(index);
                            if (rs.wasNull()){
                                b = null;
                            }
                            return b;
                        } else {
                            Integer i = rs.getInt(index);
                            if (rs.wasNull()){
                                i = null;
                            }

                            return i;
                        }
                    default:
                        Integer i = rs.getInt(index);
                        if (rs.wasNull()){
                            i = null;
                        }
                        return i;
                }

            case Types.BIGINT:
                return new Integer(rs.getInt(index));
            case Types.DECIMAL:
                BigDecimal decimal = rs.getBigDecimal(index);
                if (decimal == null) {
                    return null;
                } else {
                    return decimal.intValue();
                }
            case Types.DATE:
            case Types.TIMESTAMP:
                Timestamp timestamp = rs.getTimestamp(index);
                if (timestamp == null) {
                    return null;
                } else {
                    return new java.util.Date(timestamp.getTime());
                }
            case Types.ARRAY:
            case Types.BIT:
            case Types.BOOLEAN:
            case Types.CHAR:
            case Types.DATALINK:
            case Types.DISTINCT:
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.INTEGER:
            case Types.JAVA_OBJECT:
            case Types.LONGNVARCHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCLOB:
            case Types.NULL:
            case Types.NVARCHAR:
            case Types.OTHER:
            case Types.REAL:
            case Types.REF:
            case Types.ROWID:
            case Types.SMALLINT:
            case Types.SQLXML:
            case Types.STRUCT:
            case Types.TIME:
            case Types.TINYINT:
                return rs.getObject(index);
            default:
                return rs.getObject(index);
        }

    }

    /**
     * 插入记录
     *
     * @param conn
     *            数据库连接
     * @param table
     *            表名
     * @param dataMap
     *            插入数据的Map，其中的key就是字段名，value是对应该字段的数据
     * @throws Exception
     */
    @Override
    public int insert(Connection conn, String table, Map<String, Object> dataMap)
            throws Exception {
        return this.insert(conn, table, dataMap, false);
    }

    /**
     * 插入记录
     *
     * @param conn
     *            数据库连接
     * @param table
     *            表名
     * @param dataMap
     *            插入数据的Map，其中的key就是字段名，value是对应该字段的数据
     * @param isAutoGenKey
     *            是否自增
     * @throws Exception
     */
    @Override
    public int insert(Connection conn, String table,
                      Map<String, Object> dataMap, boolean isAutoGenKey) throws Exception {
        return this.insert(conn, table, dataMap, null, isAutoGenKey);
    }

    /**
     * 插入记录
     *
     * @param conn
     *            数据库连接
     * @param table
     *            表名
     * @param dataMap
     *            插入数据的Map，其中的key就是字段名，value是对应该字段的数据
     * @param colList
     *            插入完成后需要返回的字段名称
     * @param isAutoGenKey
     *            是否自增
     * @throws Exception
     */
    @Override
    public int insert(Connection conn, String table,
                      Map<String, Object> dataMap, String[] colList, boolean isAutoGenKey)
            throws Exception {
        long t1 = System.currentTimeMillis();
        // 检查输入参数
        if (conn == null || table == null || dataMap == null) {
            String err = "调用insert参数错误";
            log.error(err);
            throw new IllegalArgumentException(err);
        }

        // 构造插入语句，形如：insert into tb_test (name, type, id) values (?, ?, ?)
        Set<Map.Entry<String, Object>> entrySet = dataMap.entrySet();
        StringBuffer sql = new StringBuffer(64 + entrySet.size() * 8);
        sql.append("insert into " + table + " (");

        List<Object> values = new ArrayList<Object>(entrySet.size());
        for (Map.Entry<String, Object> entry : entrySet) {
            if (values.size() > 0) {
                sql.append(",");
            }
            sql.append(entry.getKey());
            values.add(entry.getValue());
        }
        sql.append(") values (");

        for (int i = 0; i < entrySet.size(); i++) {
            if (i > 0) {
                sql.append(",");
            }
            sql.append("?");
        }
        sql.append(")");

        // 设置参数
        PreparedStatement pstmt = null;
        if (isAutoGenKey) {
            if (colList != null){
                pstmt = conn.prepareStatement(sql.toString(), colList);
            }
            else {
                if (getDbType(conn).toLowerCase().contains("oracle")){
                    pstmt = conn.prepareStatement(sql.toString(),
                            new String[] { getTableId(conn, table) });
                }
                else{
                    pstmt = conn.prepareStatement(sql.toString(),
                            Statement.RETURN_GENERATED_KEYS);
                }
            }
        } else {
            pstmt = conn.prepareStatement(sql.toString());
        }

        preparedStatementSetValues(pstmt, values);

        // 执行
        ResultSet rs = null;

        int autoIncreaseId = 0;
        try {
            pstmt.executeUpdate();
            if (isAutoGenKey) {
                rs = pstmt.getGeneratedKeys();
                if (rs.next()) {
                    autoIncreaseId = rs.getInt(1);
                }
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
            pstmt.close();
            if (log.isDebugEnabled()) {
                long t2 = System.currentTimeMillis();
                log.debug("insert SQL: " + sql + ";time consuming:" + (t2 - t1));
            }
        }

        return autoIncreaseId;
    }

    /**
     * 更新表记录的多个字段
     *
     * @param conn
     *            数据库连接
     * @param table
     *            表名
     * @param dataMap
     *            待更新数据的Map，其中的key就是字段名，value是对应该字段的数据
     * @param condition
     *            条件语句
     * @param condValues
     *            条件参数列表
     * @throws Exception
     */
    @Override
    public void update(Connection conn, String table,
                       Map<String, Object> dataMap, String condition,
                       List<Object> condValues) throws Exception {
        long t1 = System.currentTimeMillis();
        // 检查输入参数
        if (conn == null || table == null || dataMap == null) {
            String err = "调用update参数错误";
            log.error(err);
            throw new IllegalArgumentException(err);
        }

        // 构造更新语句，形如：update tb_test set name=?, type=?, id=? where xx
        Set<Map.Entry<String, Object>> entrySet = dataMap.entrySet();
        StringBuffer sql = new StringBuffer(64 + entrySet.size() * 8);
        sql.append("update " + table + " set ");

        List<Object> values = new ArrayList<Object>(entrySet.size());
        for (Map.Entry<String, Object> entry : entrySet) {
            if (values.size() > 0){
                sql.append(",");
            }
            sql.append(entry.getKey() + "=?");
            values.add(entry.getValue());
        }

        if (condition != null){
            sql.append(" where " + condition);
        }

        // 设置参数
        PreparedStatement pstmt = conn.prepareStatement(sql.toString());
        if (condition != null && condValues != null) {
            // 如果有条件参数，加在后面
            values.addAll(condValues);
        }
        preparedStatementSetValues(pstmt, values);

        // 执行
        try {
            pstmt.executeUpdate();
        } finally {
            pstmt.close();
            if (log.isDebugEnabled()) {
                long t2 = System.currentTimeMillis();
                log.debug("update SQL: " + sql + ";time consuming:" + (t2 - t1));
            }

        }
    }
    /**
     * 更新表记录的一个字段
     *
     * @param conn
     *            数据库连接
     * @param table
     *            表名
     * @param name
     *            待更新字段名
     * @param value
     *            待更新数据
     * @param condition
     *            无参数的条件语句
     * @throws Exception
     */
    @Override
    public void update(Connection conn, String table, String name,
                       Object value, String condition) throws Exception {
        long t1 = System.currentTimeMillis();
        // 检查输入参数
        if (conn == null || table == null || name == null || value == null
                || condition == null) {
            String err = "调用update参数错误";
            log.error(err);
            throw new IllegalArgumentException("调用update参数错误");
        }

        // 构造更新语句，形如：update tb_test set name=?
        StringBuffer sql = new StringBuffer(64);
        sql.append("update " + table + " set " + name + "=?");
        sql.append(" where " + condition);

        // 设置参数
        PreparedStatement pstmt = conn.prepareStatement(sql.toString());
        // pstmt.setObject(1, value);
        // TimeStamp类型，不能使用new Date进行
        List<Object> values = new ArrayList<Object>();
        values.add(value);
        preparedStatementSetValues(pstmt, values);

        // 执行
        try {
            pstmt.executeUpdate();
        } finally {
            pstmt.close();
            if (log.isDebugEnabled()) {
                long t2 = System.currentTimeMillis();
                log.debug("update SQL: " + sql + ";time consuming:" + (t2 - t1));
            }
        }
    }

    /**
     * 删除记录
     *
     * @param conn
     *            数据库连接
     * @param table
     *            表名
     * @param condition
     *            条件语句
     * @param condValues
     *            条件参数列表
     * @throws Exception
     */
    @Override
    public void delete(Connection conn, String table, String condition,
                       List<Object> condValues) throws Exception {
        long t1 = System.currentTimeMillis();
        if (conn == null || table == null) {
            String err = "调用delete参数错误";
            log.error(err);
            throw new IllegalArgumentException(err);
        }

        String sql;
        if (condition == null)
            sql = "delete from " + table;
        else
            sql = "delete from " + table + " where " + condition;

        PreparedStatement pstmt = conn.prepareStatement(sql);

        if (condValues != null) {
            preparedStatementSetValues(pstmt, condValues);
        }
        try {
            pstmt.executeUpdate();
        } finally {
            pstmt.close();
            if (log.isDebugEnabled()) {
                long t2 = System.currentTimeMillis();
                log.debug("delete SQL: " + sql + ";time consuming:" + (t2 - t1));
            }
        }
    }

    /**
     * 获取一条记录，以map返回
     *
     * @param conn
     *            数据库连接
     * @param table
     *            表名
     * @param cols
     *            需要返回的字段名列表
     * @param condition
     *            条件语句
     * @param condValues
     *            条件参数列表
     * @param mustUnique
     *            是否必须唯一？若为True，但实际又有多条记录，将抛出异常
     * @return 数据map
     * @throws Exception
     */
    @Override
    public Map<String, Object> getOneRowAsMap(Connection conn, String table,
                                              List<String> cols, String condition, List<Object> condValues,
                                              boolean mustUnique) throws Exception {
        Map<String, Object> map = new HashMap<String, Object>();

        getOneRow(conn, table, cols, condition, condValues, mustUnique, map);
        return map;
    }

    /**
     * 获取一条记录，以数组方式返回。
     *
     * @param conn
     *            数据库连接
     * @param table
     *            表名
     * @param cols
     *            需要返回的字段名列表
     * @param condition
     *            条件语句
     * @param condValues
     *            条件参数列表
     * @param mustUnique
     *            是否必须唯一？若为True，但实际又有多条记录，将抛出异常
     * @return
     * @throws Exception
     */
    @Override
    public Object[] getOneRowAsArray(Connection conn, String table,
                                     List<String> cols, String condition, List<Object> condValues,
                                     boolean mustUnique) throws Exception {
        return getOneRow(conn, table, cols, condition, condValues, mustUnique,
                null);
    }

    /**
     * 获取一条数据的实现，根据map参数决定以map还是数组方式返回
     *
     * @param conn
     *            数据库连接
     * @param table
     *            表名
     * @param cols
     *            需要返回的字段名列表
     * @param condition
     *            条件语句
     * @param condValues
     *            条件参数列表
     * @param mustUnique
     *            是否必须唯一？若为True，但实际又有多条记录，将抛出异常
     * @param map
     *            若不为null，设置返回数据到其中；否则以数组返回
     * @return 若map不为null，设置返回数据到map，返回null；否则以数组返回
     * @throws Exception
     */
    protected Object[] getOneRow(Connection conn, String table,
                                 List<String> cols, String condition, List<Object> condValues,
                                 boolean mustUnique, Map<String, Object> map) throws Exception {
        long t1 = System.currentTimeMillis();
        if (conn == null || table == null) {
            String err = "调用getOneRow参数错误";
            log.error(err);
            throw new IllegalArgumentException(err);
        }

        StringBuffer sql = new StringBuffer(32);

        int i;
        if (cols == null)
            sql.append("select * from ");
        else {
            sql.append("select ");
            for (i = 0; i < cols.size(); i++) {
                if (i > 0)
                    sql.append(",");
                sql.append(cols.get(i));
            }
            sql.append(" from ");
        }
        sql.append(table);

        if ((condition != null) && (condition.length() > 0)) {
            sql.append(" where " + condition);
        }

        PreparedStatement pstmt = conn.prepareStatement(sql.toString());
        try {
            if (condValues != null) {
                preparedStatementSetValues(pstmt, condValues);
            }

            ResultSet rs = pstmt.executeQuery();

            if (!rs.next()) {
                // 无数据
                String err = "未发现要查找的数据。[" + table + ":" + sql;
                log.error(err);
                throw new Exception(err);
            }

            Object[] resArray = null;
            if (map != null) {
                ResultSetMetaData rsmd = rs.getMetaData();
                for (i = 1; i <= rsmd.getColumnCount(); i++) {
                    map.put(rsmd.getColumnName(i), getRsObj(i, rs, rsmd));
                }
            } else {
                ResultSetMetaData rsmd = rs.getMetaData();
                resArray = new Object[rsmd.getColumnCount()];
                for (i = 1; i <= rsmd.getColumnCount(); i++) {
                    resArray[i - 1] = getRsObj(i, rs, rsmd);
                }
            }

            if (mustUnique) {
                if (rs.next()) {
                    String err = "所查找的数据项不唯一。[" + table + ":" + sql;
                    log.error(err);
                    throw new Exception(err);
                }
            }
            if (map != null) {
                return null;
            } else {
                return resArray;
            }
        } finally {
            if (pstmt != null) {
                pstmt.close();
            }
            if (log.isDebugEnabled()) {
                long t2 = System.currentTimeMillis();
                log.debug("getOneRow SQL: " + sql + ";time consuming:"
                        + (t2 - t1));
            }
        }
    }

    /**
     * 通用查询，以List[map1, map2...]形式返回
     *
     * @param conn
     *            数据库连接
     * @param table
     *            表名
     * @param cols
     *            需要返回的字段名列表
     * @param condition
     *            条件语句
     * @param condValues
     *            条件参数列表
     * @param start
     *            记录的开始位置
     * @param max
     *            返回最大记录个数
     * @param orderBy
     *            排序字段名
     * @param groupBy
     *            分组字段名
     * @param bAsc
     *            是否升序
     * @return 以List[map1, map2...]形式返回，cols指定的字段数据，最大为max个
     * @throws Exception
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> searchAsMapList(Connection conn,
                                                     String table, List<String> cols, String condition,
                                                     List<Object> condValues, int start, int max, String orderBy,
                                                     String groupBy, boolean bAsc) throws Exception {
        return (List<Map<String, Object>>) search(conn, table, cols, condition,
                condValues, start, max, orderBy, groupBy, bAsc, true);
    }

    // 添加一个以数据库Label为key的方法,考虑修改会影响其他人所以另写在一个方法,希望在项目空窗期合并起来 by maj 2016-8-8
    @Override
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> searchAsMapListInLabel(Connection conn,
                                                            String table, List<String> cols, String condition,
                                                            List<Object> condValues, int start, int max, String orderBy,
                                                            String groupBy, boolean bAsc) throws Exception {
        return (List<Map<String, Object>>) searchInLabel(conn, table, cols,
                condition, condValues, start, max, orderBy, groupBy, bAsc, true);
    }

    /**
     * 通用查询，以List[array1, array2...]形式返回
     *
     * @param conn
     *            数据库连接
     * @param table
     *            表名
     * @param cols
     *            需要返回的字段名列表
     * @param condition
     *            条件语句
     * @param condValues
     *            条件参数列表
     * @param start
     *            记录的开始位置
     * @param max
     *            返回最大记录个数
     * @param orderBy
     *            排序字段名
     * @param groupBy
     *            分组字段名
     * @param bAsc
     *            是否升序
     * @return 以List[array1, array2...]形式返回，cols指定的字段数据
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    @Override
    public List<Object[]> searchAsArrayList(Connection conn, String table,
                                            List<String> cols, String condition, List<Object> condValues,
                                            int start, int max, String orderBy, String groupBy, boolean bAsc)
            throws Exception {
        return (List<Object[]>) search(conn, table, cols, condition,
                condValues, start, max, orderBy, groupBy, bAsc, false);
    }

    /**
     * 查询的具体实现，根据最后参数决定是以map-list还是array-list返回
     *
     * @param conn
     *            数据库连接
     * @param table
     *            表名
     * @param cols
     *            需要返回的字段名列表
     * @param condition
     *            条件语句
     * @param condValues
     *            条件参数列表
     * @param start
     *            记录的开始位置
     * @param max
     *            返回最大记录个数
     * @param orderBy
     *            排序字段名
     * @param groupBy
     *            分组字段名
     * @param bAsc
     *            是否升序
     * @param bMap
     *            true: 以List[map1, map1...]形式返回; false: 以List[array1,
     *            array2...]形式返回
     * @return 由bMap参数决定
     * @throws Exception
     */
    protected List<?> search(Connection conn, String table, List<String> cols,
                             String condition, List<Object> condValues, int start, int max,
                             String orderBy, String groupBy, boolean bAsc, boolean bMap)
            throws Exception {
        long t1 = System.currentTimeMillis();
        if (conn == null || table == null) {
            String err = "调用search参数错误";
            log.error(err);
            throw new IllegalArgumentException(err);
        }

        // 1 构造语句
        StringBuilder sql = new StringBuilder(64);
        int i;
        if (cols == null)
            sql.append("select * from ");
        else {
            sql.append("select ");
            for (i = 0; i < cols.size(); i++) {
                if (i > 0){
                    sql.append(",");
                }
                sql.append(cols.get(i));
            }
            sql.append(" from ");
        }
        sql.append(table);

        if ((condition != null) && (condition.length() > 0)) {
            sql.append(" where " + condition);
        }

        if (groupBy != null) {
            sql.append(" group by " + groupBy);
        }

        if (orderBy != null) {
            sql.append(" order by " + orderBy);
            if (!bAsc)
                sql.append(" desc");
        }

        PreparedStatement pstmt = conn.prepareStatement(sql.toString());

        if (condValues != null) {
            preparedStatementSetValues(pstmt, condValues);
        }

        // 2 执行查询
        List<Object> ret = new ArrayList<Object>();
        try {
            ResultSet rs = pstmt.executeQuery();

            // 3 构造结果
            int colCount = 0;
            ResultSetMetaData rsmd = rs.getMetaData();
            colCount = rsmd.getColumnCount();

            int startCounter = 0;
            int fetchCount = 0;
            if (bMap) {
                // map结果
                while (rs.next()) {
                    if (fetchCount == max){
                        break;
                    }

                    if (startCounter >= start) {
                        Map<String, Object> oneRec = new HashMap<String, Object>();
                        for (i = 1; i <= colCount; i++) {
                            oneRec.put(rsmd.getColumnName(i),
                                    getRsObj(i, rs, rsmd));
                        }
                        ret.add(oneRec);
                        fetchCount++;
                    } else {
                        startCounter++;
                    }
                }
            } else {
                // array结果
                while (rs.next()) {
                    if (fetchCount == max)
                        break;

                    if (startCounter >= start) {
                        Object[] oneRec = new Object[colCount];
                        for (i = 0; i < colCount; i++) {
                            oneRec[i] = getRsObj(i + 1, rs, rsmd);
                        }
                        ret.add(oneRec);
                        fetchCount++;
                    } else {
                        startCounter++;
                    }
                }
            }
        } catch (Exception e) {
            throw e;
        } finally {
            if (pstmt != null)
                pstmt.close();
            if (log.isDebugEnabled()) {
                long t2 = System.currentTimeMillis();
                log.debug("search SQL: " + sql + ";time consuming:" + (t2 - t1));
            }
        }

        return ret;
    }

    // 添加一个以数据库Label为key的方法,考虑修改会影响其他人所以另写在一个方法,希望在项目空窗期合并起来 by maj 2016-8-8
    protected List<?> searchInLabel(Connection conn, String table,
                                    List<String> cols, String condition, List<Object> condValues,
                                    int start, int max, String orderBy, String groupBy, boolean bAsc,
                                    boolean bMap) throws Exception {
        long t1 = System.currentTimeMillis();
        if (conn == null || table == null) {
            String err = "调用search参数错误";
            log.error(err);
            throw new IllegalArgumentException(err);
        }

        // 1 构造语句
        StringBuffer sql = new StringBuffer(64);
        int i;
        if (cols == null){
            sql.append("select * from ");
        }
        else {
            sql.append("select ");
            for (i = 0; i < cols.size(); i++) {
                if (i > 0){
                    sql.append(",");
                }
                sql.append(cols.get(i));
            }
            sql.append(" from ");
        }
        sql.append(table);

        if ((condition != null) && (condition.length() > 0)) {
            sql.append(" where " + condition);
        }

        if (groupBy != null) {
            sql.append(" group by " + groupBy);
        }

        if (orderBy != null) {
            sql.append(" order by " + orderBy);
            if (!bAsc)
                sql.append(" desc");
        }

        PreparedStatement pstmt = conn.prepareStatement(sql.toString());

        if (condValues != null) {
            preparedStatementSetValues(pstmt, condValues);
        }

        // 2 执行查询
        List<Object> ret = new ArrayList<Object>();
        try {
            ResultSet rs = pstmt.executeQuery();

            // 3 构造结果
            int colCount = 0;
            ResultSetMetaData rsmd = rs.getMetaData();
            colCount = rsmd.getColumnCount();

            int startCounter = 0;
            int fetchCount = 0;
            if (bMap) {
                // map结果
                while (rs.next()) {
                    if (fetchCount == max){
                        break;
                    }
                    if (startCounter >= start) {
                        Map<String, Object> oneRec = new HashMap<String, Object>();
                        for (i = 1; i <= colCount; i++) {
                            oneRec.put(rsmd.getColumnLabel(i),
                                    getRsObj(i, rs, rsmd));
                        }
                        ret.add(oneRec);
                        fetchCount++;
                    } else {
                        startCounter++;
                    }
                }
            } else {
                // array结果
                while (rs.next()) {
                    if (fetchCount == max)
                        break;

                    if (startCounter >= start) {
                        Object[] oneRec = new Object[colCount];
                        for (i = 0; i < colCount; i++) {
                            oneRec[i] = getRsObj(i + 1, rs, rsmd);
                        }
                        ret.add(oneRec);
                        fetchCount++;
                    } else {
                        startCounter++;
                    }
                }
            }
        } catch (Exception e) {
            throw e;
        } finally {
            if (pstmt != null)
                pstmt.close();
            if (log.isDebugEnabled()) {
                long t2 = System.currentTimeMillis();
                log.debug("search SQL: " + sql + ";time consuming:" + (t2 - t1));
            }
        }

        return ret;

    }

    /**
     * 得到记录个数
     *
     * @param conn
     *            数据库连接
     * @param table
     *            表名
     * @param condition
     *            条件语句
     * @param condValues
     *            条件语句列表
     * @param distinct
     *            数据库distinct
     * @return 符合条件的记录个数
     * @throws Exception
     */
    @Override
    public int getCount(Connection conn, String table, String condition,
                        List<Object> condValues, String distinct) throws Exception {
        long t1 = System.currentTimeMillis();
        if (conn == null || table == null) {
            String err = "调用getCount参数错误";
            log.error(err);
            throw new IllegalArgumentException(err);
        }

        StringBuffer sql = new StringBuffer(32);
        if (distinct == null) {
            sql.append("select count(*) from " + table);
        }
        else {
            sql.append("select count(distinct " + distinct + ") from " + table);
        }

        if (EmptyUtils.isNotEmpty(condition)) {
            sql.append(SQLUtil.WHERE + condition);
        }

        PreparedStatement pstmt = conn.prepareStatement(sql.toString());

        if (condValues != null) {
            preparedStatementSetValues(pstmt, condValues);
        }
        try {

            ResultSet rs = pstmt.executeQuery();

            if (!rs.next()) // 无数据
                return 0;

            return rs.getInt(1);
        } finally {
            pstmt.close();
            if (log.isDebugEnabled()) {
                long t2 = System.currentTimeMillis();
                log.debug("getCount SQL: " + sql + ";time consuming:"
                        + (t2 - t1));
            }
        }
    }

    /**
     * 得到记录个数
     *
     * @param type
     *            数据类型
     * @param conn
     *            数据库连接
     * @param table
     *            表名
     * @param condition
     *            条件语句
     * @param condValues
     *            条件语句列表
     * @return 符合条件的记录个数
     * @throws Exception
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T getMax(Class<T> type, Connection conn, String table,
                        String col, String condition, List<Object> condValues)
            throws Exception {
        long t1 = System.currentTimeMillis();
        if (conn == null || table == null) {
            String err = "调用getCount参数错误";
            log.error(err);
            throw new IllegalArgumentException(err);
        }

        StringBuffer sql = new StringBuffer(32);
        sql.append("select max(" + col + ") from " + table);

        if (condition != null){
            sql.append(" where " + condition);
        }


        PreparedStatement pstmt = conn.prepareStatement(sql.toString());
        if (condValues != null) {
            preparedStatementSetValues(pstmt, condValues);
        }
        try {
            ResultSet rs = pstmt.executeQuery();
            if (!rs.next()) // 无数据
                return null;
            return ((T) rs.getObject(1));
        } finally {
            pstmt.close();
            if (log.isDebugEnabled()) {
                long t2 = System.currentTimeMillis();
                log.debug("getMax SQL: " + sql + ";time consuming:" + (t2 - t1));
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getMin(Class<Integer> type, Connection conn, String table,
                        String col, String condition, List<Object> condValues)
            throws Exception {
        long t1 = System.currentTimeMillis();
        if (conn == null || table == null) {
            String err = "调用getCount参数错误";
            log.error(err);
            throw new IllegalArgumentException(err);
        }

        StringBuffer sql = new StringBuffer(32);
        sql.append("select min(" + col + ") from " + table);

        if (condition != null) {
            sql.append(" where " + condition);
        }

        PreparedStatement pstmt = conn.prepareStatement(sql.toString());
        if (condValues != null) {
            preparedStatementSetValues(pstmt, condValues);
        }
        try {
            ResultSet rs = pstmt.executeQuery();
            // 无数据
            if (!rs.next()) {
                return null;
            }
            return ((T) rs.getObject(1));
        } finally {
            pstmt.close();
            long t2 = System.currentTimeMillis();
            if (log.isDebugEnabled()) {
                log.debug("getMin SQL: " + sql + ";time consuming:" + (t2 - t1));
            }
        }

    }

    /**
     * 直接批量执行sql语句，不返回任何值
     */
    @Override
    public void execSQL(Connection conn, String[] sqls) throws Exception {
        Statement statement = conn.createStatement();
        try {
            // 分批次执行
            // 一次执行的命令数
            int linePerTime = 30;
            int remainder = sqls.length % linePerTime;
            int loopTime = (sqls.length / linePerTime)
                    + (remainder > 0 ? 1 : 0);
            for (int i = 0; i < loopTime; i++) {
                statement.clearBatch();
                String[] childSqls = Arrays.copyOfRange(sqls, 0 + linePerTime
                        * i, linePerTime * i
                        + (i == (loopTime - 1) ? remainder : linePerTime));
                for (String sql : childSqls) {
                    statement.addBatch(sql);
                }
                statement.executeBatch();
            }

        } finally {
            statement.close();
        }
    }

    /**
     * 统计 SQL预计执行时间
     *
     * @param conn
     * @param sqls
     * @return
     * @throws Exception
     */
    public long execSQLTime(Connection conn, String sqls) throws Exception {
        long t1 = System.currentTimeMillis();

        long t2 = System.currentTimeMillis();

        return (t2 - t1);
    }

    @Override
    public int insertIfNotExists(Connection conn, String table,
                                 Map<String, Object> dataMap, String condition,
                                 List<Object> condValues) throws Exception {
        if (condition != null
                && (getCount(conn, table, condition, condValues, null) > 0)) {
            throw new Exception("记录已经存在");
        }
        return insert(conn, table, dataMap, true);
    }

    private String getTableId(Connection conn, String tableName)
            throws SQLException {
        String result = null;
        String userName = conn.getMetaData().getUserName();
        String dbType = getDbType(conn).toLowerCase();
        ResultSet rs = conn.getMetaData().getPrimaryKeys(null,
                getLegalUserName(userName, dbType), tableName);
        while (rs.next()) {
            result = (String) rs.getObject("COLUMN_NAME");
        }
        return result;
    }

    private String getDbType(Connection conn) throws SQLException {
        return conn.getMetaData().getDatabaseProductName();
    }

    private String getLegalUserName(String user, String type) {
        String result;
        if (user != null) {
            if (type.equals("oracle")){
                result = user.toUpperCase();
            }else{
                result = user;
            }
        } else{
            result = "public";
        }
        return result;
    }
}
