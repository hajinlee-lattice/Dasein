package com.latticeengines.dellebi.entitymanager.impl;

import java.sql.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.dellebi.dao.DellEbiConfigDao;
import com.latticeengines.dellebi.entitymanager.DellEbiConfigEntityMgr;
import com.latticeengines.domain.exposed.dellebi.DellEbiConfig;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("dellEbiConfigEntityMgrImpl")
public class DellEbiConfigEntityMgrImpl extends BaseEntityMgrImpl<DellEbiConfig>implements DellEbiConfigEntityMgr {

    @Autowired
    private DellEbiConfigDao dellEbiConfigDao;

    private List<DellEbiConfig> configs;

    private static final Log log = LogFactory.getLog(DellEbiConfigEntityMgrImpl.class);

    @Override
    @Transactional(value = "transactionManagerDellEbiCfg", propagation = Propagation.REQUIRED)
    public void initialService() {
        List<DellEbiConfig> configslist;
        configslist = dellEbiConfigDao.queryConfigs();
        setConfigs(configslist);
    }

    @Override
    public String getInputFields(String type) {

        if (type == null) {
            throw new LedpException(LedpCode.LEDP_29001);
        }

        DellEbiConfig config = getConfigByType(type);

        return config.getInputFields();

    }

    @Override
    public String getOutputFields(String type) {

        if (type == null) {
            throw new LedpException(LedpCode.LEDP_29001);
        }

        DellEbiConfig config = getConfigByType(type);

        return config.getOutputFields();
    }

    @Override
    public String getHeaders(String type) {

        if (type == null) {
            throw new LedpException(LedpCode.LEDP_29001);
        }

        DellEbiConfig config = getConfigByType(type);

        return config.getHeaders();
    }

    @Override
    public String getTargetColumns(String type) {

        if (type == null) {
            throw new LedpException(LedpCode.LEDP_29001);
        }

        DellEbiConfig config = getConfigByType(type);

        return config.getTargetColumns();
    }

    @Override
    public BaseDao<DellEbiConfig> getDao() {

        return dellEbiConfigDao;
    }

    @Override
    public List<DellEbiConfig> getConfigs() {
        return configs;
    }

    public void setConfigs(List<DellEbiConfig> cfgList) {
        configs = cfgList;
    }

    @Override
    public DellEbiConfig getConfigByType(String type) {

        for (DellEbiConfig config : configs) {
            if (config.getType().equalsIgnoreCase(type)) {
                return config;
            }
        }
        log.warn("Type " + type + " is not defined in the config table.");
        return null;
    }

    @Override
    public DellEbiConfig getConfigByBean(String bean) {

        for (DellEbiConfig config : configs) {
            if (config.getBean().equalsIgnoreCase(bean)) {
                return config;
            }
        }

        throw new LedpException(LedpCode.LEDP_29005, new String[] { bean });
    }

    @Override
    public DellEbiConfig getConfigByFileName(String fileName) {

        for (DellEbiConfig config : configs) {
            String regexStr = config.getFilePattern().replace("*", ".*");
            regexStr = regexStr.replace("?", ".");
            if (fileName.matches(regexStr)) {
                return config;
            }
        }

        throw new LedpException(LedpCode.LEDP_29006, new String[] { fileName });
    }

    @Override
    public Date getStartDate(String type) {
        if (type == null) {
            throw new LedpException(LedpCode.LEDP_29001);
        }

        DellEbiConfig config = getConfigByType(type);

        return config.getStartDate();
    }

    @Override
    public String getTargetTable(String type) {

        if (type == null) {
            throw new LedpException(LedpCode.LEDP_29001);
        }

        DellEbiConfig config = getConfigByType(type);

        return config.getTargetTable();
    }

    @Override
    public Boolean getIsDeleted(String type) {

        if (type == null) {
            throw new LedpException(LedpCode.LEDP_29001);
        }

        DellEbiConfig config = getConfigByType(type);

        return (config.getIsDeleted() == null ? false : config.getIsDeleted());
    }

    @Override
    public Boolean getIsActive(String type) {

        if (type == null) {
            throw new LedpException(LedpCode.LEDP_29001);
        }

        DellEbiConfig config = getConfigByType(type);

        return (config.getIsActive() == null ? false : config.getIsActive());
    }

    @Override
    public String getInboxPath(String type) {

        if (type == null) {
            throw new LedpException(LedpCode.LEDP_29001);
        }

        DellEbiConfig config = getConfigByType(type);

        return config.getInboxPath();
    }

    @Override
    public String getBean(String type) {

        if (type == null) {
            throw new LedpException(LedpCode.LEDP_29001);
        }

        DellEbiConfig config = getConfigByType(type);

        return config.getBean();

    }

    @Override
    public String getFilePattern(String type) {
        if (type == null) {
            throw new LedpException(LedpCode.LEDP_29001);
        }

        DellEbiConfig config = getConfigByType(type);

        return config.getFilePattern();
    }

    @Override
    public int getPriority(String type) {
        if (type == null) {
            throw new LedpException(LedpCode.LEDP_29001);
        }

        DellEbiConfig config = getConfigByType(type);

        return config.getPriority();
    }

    @Override
    public String getTypeByBean(String bean) {
        if (bean == null) {
            throw new LedpException(LedpCode.LEDP_29001);
        }

        DellEbiConfig config = getConfigByBean(bean);

        return config.getType();
    }

    @Override
    public String getTypeByFileName(String fileName) {
        if (fileName == null) {
            throw new LedpException(LedpCode.LEDP_29001);
        }

        DellEbiConfig config = getConfigByFileName(fileName);

        return config.getType();
    }

    @Override
    public String getPostStoreProcedure(String type) {
        if (type == null) {
            throw new LedpException(LedpCode.LEDP_29001);
        }

        DellEbiConfig config = getConfigByType(type);

        return config.getPostStoreProcedure();
    }
}
