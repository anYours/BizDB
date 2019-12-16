package com.common.util;

import com.common.db.ext.DbEnums;
import com.common.db.ext.IDbCfg;

import java.io.File;
import java.io.IOException;

/**
 * 生成数据库连接的URL
 *
 * @author xujun
 *
 */
public class DbUrlUtils {

	/**
	 * 默认的Firebird数据库存储路径前缀，用于Firebird采用相对路径表达时
 	 */
	static String DEFAULT_GDB_PATH_PREFIX;

	public static void setDefaultGdbPathPreFix(String path) {
		DEFAULT_GDB_PATH_PREFIX = path;
	}

	public static String genUrl(IDbCfg dbCfg) {
		// 尝试获取数据库连接
		String urlPattern = "";
		String actualPath = dbCfg.getDbNamePath();
		switch (dbCfg.getDbType()) {
			case ORACLE_SERVICE_NAME:
				urlPattern = "jdbc:oracle:thin:@//${ip}:${port}/${path}";
				break;
			case ORACLE:
				urlPattern = "jdbc:oracle:thin:@${ip}:${port}:${path}";
				break;
			case ORACLE_RAC:
				urlPattern = "jdbc:oracle:thin:@(DESCRIPTION =(ADDRESS = (PROTOCOL = TCP)(HOST = ${ip})(PORT = ${port}))(ADDRESS = (PROTOCOL = TCP)(HOST = ${ip_rac})(PORT = ${port_rac}))(LOAD_BALANCE = yes)(FAILOVER = ON)(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = ${path})(FAILOVER_MODE=(TYPE = SELECT)(METHOD = BASIC)(RETIRES = 20)(DELAY = 15))))";
				break;
			case DB2:
				urlPattern = "jdbc:db2://${ip}:${port}/${path}";
				break;
			case SYBASE:
				// cp936代表连接是GBK
				urlPattern = "jdbc:sybase:Tds:${ip}:${port}/${path}?charset=cp936";
				break;
			case GBASE:
				urlPattern = "jdbc:gbase://${ip}:${port}/${path}";
				break;
			case MSSQL:
				urlPattern = "jdbc:sqlserver://${ip}:${port};databaseName=${path}";
				break;
			case MYSQL:
				// zeroDateTimeBehavior=convertToNull 防止在 DATETIME 类型的字段默认值为“0000-00-00 00:00:00”，java获取的时候会报转换Date出错
				urlPattern = "jdbc:mysql://${ip}:${port}/${path}?useUnicode=true&characterEncoding=${charSet}&useSSL=false&serverTimezone=UTC";
				break;
			case FIREBIRD:
				urlPattern = "jdbc:firebirdsql:${ip}/${port}:${path}";
				actualPath = getActualPath(dbCfg.getDbNamePath());
				break;
			case KINGBASE:
				urlPattern = "jdbc:kingbase://${ip}:${port}/${path}";
				break;
			case DM:
				urlPattern = "jdbc:dm://${ip}:${port}/${path}";
				break;
			case SQLITE:
			default:
				urlPattern = "jdbc:sqlite:${path}";
				actualPath = getActualPath(dbCfg.getDbNamePath());
		}
		String ret = urlPattern.replaceFirst("\\$\\{ip\\}", dbCfg.getDbIp());
		ret = ret.replaceFirst("\\$\\{port\\}", dbCfg.getDbPort()+"");
		if(ret.indexOf("charSet") > -1) {
			ret = ret.replaceFirst("\\$\\{charSet\\}", dbCfg.getDbCharSet());
		}
		ret = ret.replaceFirst("\\$\\{path\\}",actualPath.replaceAll("\\\\", "\\\\\\\\"));
		switch (dbCfg.getDbType()) {
			case ORACLE_RAC:
				ret = ret.replaceFirst("\\$\\{ip_rac\\}", dbCfg.getExtParam().get("RAC_IP"));
				ret = ret.replaceFirst("\\$\\{port_rac\\}", dbCfg.getExtParam().get("RAC_PORT"));
				break;
			default:
				break;
		}
		return ret;

	}

	/**
	 * 针对本地文件数据库采用相对路径，如"./db/firebird.gdb"类似的情况<br>
	 * 将相对路径转换为绝对路径
	 * @param dbNamePath　相对路径
	 * @return 绝对路径
	 */
	private static String getActualPath(String dbNamePath)
	{
		String actualPath = null;
		if (dbNamePath.startsWith(".")) {
			// 如果这是一个相对路径
			if (DEFAULT_GDB_PATH_PREFIX != null) {
				// 如果已预设置了前缀路径
				String filePath = DEFAULT_GDB_PATH_PREFIX
						+ File.separatorChar + dbNamePath;
				try {
					actualPath = new File(filePath).getCanonicalPath();
				} catch (IOException e) {
					//e.printStackTrace();
				}
			}
			if (actualPath == null) {
				// 如果已预前缀路径为空或者组合路径错误，则要尝试直接转换
				actualPath = new File(dbNamePath).getAbsolutePath();
			}
		} else {//如果是绝对路径，直接等
			actualPath = dbNamePath;
		}
		// if (!new File(actualPath).exists()) {
		// throw new RuntimeException("错误的Firebird数据库路径："+dbNamePath);
		// }
		return actualPath;
	}

	public static String getDriver(DbEnums.DBType dbType) {
		String jdbcDriver = "";
		switch (dbType) {
			case ORACLE_SERVICE_NAME:
			case ORACLE:
			case ORACLE_RAC:
				jdbcDriver = "oracle.jdbc.driver.OracleDriver";
				break;
			case DB2:
				jdbcDriver = "com.ibm.db2.jcc.DB2Driver";
				break;
			case SYBASE:
				jdbcDriver = "com.sybase.jdbc3.jdbc.SybDriver";
				break;
			case GBASE:
				jdbcDriver = "com.gbase.jdbc.Driver";
				break;
			case MSSQL:
				jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
				break;
			case MYSQL:
				jdbcDriver = "com.mysql.jdbc.Driver";
				break;
			case FIREBIRD:
				jdbcDriver = "org.firebirdsql.jdbc.FBDriver";
				break;
			case KINGBASE:
				jdbcDriver = "com.kingbase.Driver";
				break;
			case DM:
				jdbcDriver = "dm.jdbc.driver.DmDriver";
				break;
			case SQLITE:
			default:
				jdbcDriver = "org.sqlite.JDBC";
		}
		return jdbcDriver;
	}

	public static String getSqlFile(DbEnums.DBType dbType, String sqlFilePath) {
		String fileName = "";
		switch (dbType) {
			case ORACLE:
			case ORACLE_RAC:
				fileName = "oracle.sql";
				break;
			case DB2:
				fileName = "db2.sql";
				break;
			case SYBASE:
				fileName = "sybase.sql";
				break;
			case GBASE:
				fileName = "gbase.sql";
				break;
			case MSSQL:
				fileName = "mssql.sql";
				break;
			case MYSQL:
				fileName = "mysql.sql";
				break;
			case FIREBIRD:
				fileName = "firebird.sql";
				break;
			case KINGBASE:
				fileName = "kingbase.sql";
				break;
			case DM:
				fileName = "dm.sql";
				break;
			case SQLITE:
			default:
				fileName = "sqlite.sql";
		}
		String sqlFile = sqlFilePath + File.separatorChar + fileName;
		return sqlFile;
	}

	public static String getSqlFile(DbEnums.DBType dbType) {
		String fileName = "";
		switch (dbType) {
			case ORACLE:
			case ORACLE_RAC:
				fileName = "oracle.sql";
				break;
			case DB2:
				fileName = "db2.sql";
				break;
			case SYBASE:
				fileName = "sybase.sql";
				break;
			case GBASE:
				fileName = "gbase.sql";
				break;
			case MSSQL:
				fileName = "mssql.sql";
				break;
			case MYSQL:
				fileName = "mysql.sql";
				break;
			case FIREBIRD:
				fileName = "firebird.sql";
				break;
			case KINGBASE:
				fileName = "kingbase.sql";
				break;
			case DM:
				fileName = "dm.sql";
				break;
			case SQLITE:
			default:
				fileName = "sqlite.sql";
		}
		return fileName;
	}
}
