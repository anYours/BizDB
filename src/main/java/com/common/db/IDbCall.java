package com.common.db;

import java.util.List;
import java.util.Map;

/**
 * 与connection无关的数据库基本CRUD操作接口，WCL某些组件依赖此接口实现数据库操作。函数原型就是IDbOp的函数去除connection参数
 * @author renwei
 * @version 1.0
 * @since 1.0
 */
public interface IDbCall {
	/**
	 * 插入记录
	 * @param table 表名
	 * @param dataMap 插入数据的Map，其中的key就是字段名，value是对应该字段的数据
	 * @throws Exception
	 * @return 数据条数
	 */
	public int insert(String table, Map<String,Object> dataMap) throws Exception;

	/**
	 * 更新表记录的多个字段
	 * @param table 表名
	 * @param dataMap 待更新数据的Map，其中的key就是字段名，value是对应该字段的数据
	 * @param condition 条件语句
	 * @param condValues 条件参数列表
	 * @throws Exception
	 */
	public void update(String table, Map<String,Object> dataMap, String condition, List<Object> condValues) throws Exception;

	/**
	 * 更新表记录的一个字段
	 * @param table 表名
	 * @param name 待更新字段名
	 * @param value 待更新数据
	 * @param condition 无参数的条件语句
	 * @throws Exception
	 */
	public void update(String table, String name, Object value, String condition) throws Exception;

	/**
	 * 删除记录
	 * @param table 表名
	 * @param condValues 条件参数列表
	 * @throws Exception
	 */
	public void delete(String table, String preparedCond, List<Object> condValues) throws Exception;

	/**
	 * 获取一条记录，以map返回
	 * @param table 表名
	 * @param cols 需要返回的字段名列表
	 * @param condition 条件语句
	 * @param condValues 条件参数列表
	 * @param mustUnique 是否必须唯一？若为True，但实际又有多条记录，将抛出异常
	 * @return 数据map
	 * @throws Exception
	 */
	public Map<String,Object> getOneRowAsMap(String table, List<String> cols, String condition, List<Object> condValues, boolean mustUnique)
			throws Exception;

	/**
	 * 获取一条记录，以数组方式返回。
	 * @param table 表名
	 * @param cols 需要返回的字段名列表
	 * @param condition 条件语句
	 * @param condValues 条件参数列表
	 * @param mustUnique 是否必须唯一？若为True，但实际又有多条记录，将抛出异常
	 * @return
	 * @throws Exception
	 */
	public Object[] getOneRowAsArray(String table, List<String> cols, String condition, List<Object> condValues, boolean mustUnique)
			throws Exception;

	/**
	 * 通用查询，以List[map1, map2...]形式返回
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
	public List<Map<String,Object>> searchAsMapList(String table, List<String> cols, String condition, List<Object> condValues, int start, int max,
			String orderBy, String groupBy, boolean bAsc) throws Exception;

	/**
	 * 通用查询，以List[array1, array2...]形式返回
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
	public List<Object[]> searchAsArrayList(String table, List<String> cols, String condition, List<Object> condValues, int start, int max,
			String orderBy, String groupBy, boolean bAsc) throws Exception;

	/**
	 * 得到记录个数
	 * @param table 表名
	 * @param condition 条件语句
	 * @param condValues 条件语句列表
	 * @param distinct 数据库distinct
	 * @return 符合条件的记录个数
	 * @throws Exception
	 */
	public int getCount(String table, String condition, List<Object> condValues, String distinct) throws Exception;


}