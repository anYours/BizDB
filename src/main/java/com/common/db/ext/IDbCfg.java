package com.common.db.ext;

import java.util.HashMap;

/**
 * 定义基本的数据库配置接口类
 * 
 */
public interface IDbCfg {

	/**
	 * 获得数据库类型
	 *
	 * @return DbEnums
	 */
	public DbEnums.DBType getDbType();

	/**
	 * 获取数据库连接池名称
	 *
	 * @return String
	 */
	public String getConnectPoolName();

	/**
	 * 获取数据库最大活动连接数据
	 *
	 * @return 最大活动连接数据
	 */
	public int getConnectMaxActive();

	/**
	 * 获取数据库连接IP地址
	 *
	 * @return 数据库连接IP地址
	 */
	public String getDbIp();

	/**
	 * 获取数据库连接端口
	 *
	 * @return 数据库连接端口
	 */
	public int getDbPort();

	/**
	 * 获取数据库名及路径
	 *
	 * @return 数据库名及路径
	 */
	public String getDbNamePath();

	/**
	 * 获取数据库连接用户
	 *
	 * @return 数据库连接用户
	 */
	public String getDbUser();

	/**
	 * 获取数据库连接口令
	 *
	 * @return 数据库连接口令
	 */
	public String getDbPasswd();

	/**
	 * 获取数据库连接字符集
	 *
	 * @return 数据库连接字符集
	 */
	public String getDbCharSet();

	/**
	 * 获取数据库连接的扩展参数<br>
	 * 一般情况下直接返回null<br>
	 * 特别地情况，如使用Oracle RAC<br>
	 * 返回值中应该包括<br>
	 * {"RAC_IP",$StringIP}<br>
	 * {"RAC_PORT",$StringPort}<br>
	 * @return 数据库连接的扩展参数
	 */
	public HashMap<String, String> getExtParam();

}
