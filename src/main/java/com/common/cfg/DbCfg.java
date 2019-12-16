package com.common.cfg;

import com.common.db.ext.DbEnums;
import com.common.db.ext.IDbCfg;
import com.common.util.DbUrlUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;

/**
 * @Author wul
 * @Description
 * @Date 2019/12/11 14:03
 */
@Component
public class DbCfg implements IDbCfg {

    @Value("${dbcfg.dbtype}")
    public String dbtype;

    @Value("${dbcfg.poolName}")
    public String poolName;

    public int version = 5;

    @Value("${dbcfg.ip}")
    public String ip;

    @Value("${dbcfg.port}")
    public int port;

    @Value("${dbcfg.path}")
    public String path;

    /**
     * oracle应用集群的热备及其IP
     */
    public String ip_rac = "0.0.0.0";
    /**
     * oracle应用集群的热备及其端口
     */
    public int port_rac = 1521;

    /**
     * 数据库用户名
     */
    @Value("${dbcfg.user}")
    public String user;

    /**
     * 数据库密码
     */
    @Value("${dbcfg.password}")
    public String password;

    @Value("${dbcfg.charSet}")
    public String charSet;

    /**
     * 数据库连接池容量
     */
    @Value("${dbcfg.maxConnect}")
    public int maxConnect;

    /**
     * 数据库连接超时限制
     */
    @Value("${dbcfg.timeOut}")
    public int timeOut;

    /**
     * 数据库驱动类名
     */
    public String jdbcDriver ;


    @PostConstruct
    public void init(){
        this.jdbcDriver = DbUrlUtils.getDriver(getDbType());
    }

    /**
     * 获得数据库类型
     *
     * @return DbEnums
     */
    @Override
    public DbEnums.DBType getDbType() {
        DbEnums.DBType dbType = DbEnums.DBType.valueOf(this.dbtype.toUpperCase());
        return dbType;
    }

    /**
     * 获取数据库连接池名称
     *
     * @return String
     */
    @Override
    public String getConnectPoolName() {
        return poolName;
    }

    /**
     * 获取数据库最大活动连接数据
     *
     * @return 最大活动连接数据
     */
    @Override
    public int getConnectMaxActive() {
        return maxConnect;
    }

    /**
     * 获取数据库连接IP地址
     *
     * @return 数据库连接IP地址
     */
    @Override
    public String getDbIp() {
        return this.ip;
    }

    /**
     * 获取数据库连接端口
     *
     * @return 数据库连接端口
     */
    @Override
    public int getDbPort() {
        return this.port;
    }

    /**
     * 获取数据库名及路径
     *
     * @return 数据库名及路径
     */
    @Override
    public String getDbNamePath() {
        return this.path;
    }

    /**
     * 获取数据库连接用户
     *
     * @return 数据库连接用户
     */
    @Override
    public String getDbUser() {
        return this.user;
    }

    /**
     * 获取数据库连接口令
     *
     * @return 数据库连接口令
     */
    @Override
    public String getDbPasswd() {
        return this.password;
    }

    /**
     * 获取数据库连接字符集
     *
     * @return 数据库连接字符集
     */
    @Override
    public String getDbCharSet() {
        DbEnums.DBType type = DbEnums.DBType.valueOf(dbtype.toUpperCase());
        if (type == DbEnums.DBType.SYBASE) {
            charSet = null;
        }
        return charSet;
    }

    /**
     * 获取数据库连接的扩展参数<br>
     * 一般情况下直接返回null<br>
     * 特别地情况，如使用Oracle RAC<br>
     * 返回值中应该包括<br>
     * {"RAC_IP",$StringIP}<br>
     * {"RAC_PORT",$StringPort}<br>
     *
     * @return 数据库连接的扩展参数
     */
    @Override
    public HashMap<String, String> getExtParam() {
        return null;
    }

    /**
     * 获取数据库的url
     *
     * @return
     */
    public String getURL() {
        return DbUrlUtils.genUrl(this);
    }

}
