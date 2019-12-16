package com.common.db;

import com.common.cfg.DbCfg;
import com.common.db.ext.AbstractBizDB;
import com.common.util.ConfigurationPropertiesConfig;
import com.common.util.DbUrlUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @Author wul
 * @Description
 * @Date 2019/12/11 14:41
 */
@Slf4j
@Component
public class BizDB extends AbstractBizDB {

    @Autowired
    DbCfg dbCfg;

    @Autowired
    private ConfigurationPropertiesConfig configurationPropertiesConfig;

    private static final long serialVersionUID = 1L;

    private static boolean inited = false;

    @PostConstruct
    public void init(){
        if (!inited) {
            try {
                initDB();
                // 已经初始化完成
                inited = true;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    /**
     * 获得不需要初始化的实例，仅用于在配置中加载Listener
     *
     * @return
     */

    /**
     * 使用单例模式进行初始化 在未部署的情况下，需要在服务启动前调用getInstance()，<br>
     * 以保证基本的DBCfgListener的加载，这样在刚开始部署时数据库配置变化后，<br>
     * 本类才能侦听到配置变化并重新加载池
     *
     * @return
     * @throws Exception
     *//*
    public static synchronized BizDB getInstance() {
        if (!inited) {
            try {
                getInstanceWithoutInit().initDB();
                // 已经初始化完成
                inited = true;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        return getInstanceWithoutInit();
    }*/

    @Override
    public void initDB() throws Exception {
        POOL_NAME = dbCfg.getConnectPoolName();
        DbcpManager.init(POOL_NAME, DbUrlUtils.getDriver(dbCfg.getDbType()), DbUrlUtils.genUrl(dbCfg),
                dbCfg.getDbUser(), dbCfg.getDbPasswd(), dbCfg.getDbCharSet(), dbCfg.getConnectMaxActive());

        setDbopOptimize(dbCfg);
    }

    /**
     * 根据数据库配置，加载dbop优化类
     * @param dbCfg
     */
    public void setDbopOptimize(DbCfg dbCfg) {
        // 如果是达梦数据库，需要额外配置

        // 如果是已经优化过的数据库，则使用优化类
        String dbDriver = DbUrlUtils.getDriver(dbCfg.getDbType());
        DbOp.enableOptimize(dbDriver, 0);
        enableOptimize(dbDriver, 0);
    }

    String POOL_NAME = null;

    @Override
    public String getPoolName() {
        return POOL_NAME;
    }

    @Override
    public void setPoolName(String poolName) {
        POOL_NAME = poolName;
    }

    /**
     * 获取优化以后的查询类
     *
     * @return
     */
    public IDbOp getOptimizeDbOp() {
        return m_dbOp;
    }
}
