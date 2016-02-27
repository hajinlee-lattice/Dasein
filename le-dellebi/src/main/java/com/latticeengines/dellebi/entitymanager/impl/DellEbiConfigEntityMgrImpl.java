package com.latticeengines.dellebi.entitymanager.impl;

import java.sql.Date;
import java.util.List;

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

    public List<DellEbiConfig> getConfigs() {
        return configs;
    }

    public void setConfigs(List<DellEbiConfig> cfgList) {
        configs = cfgList;
    }

    public DellEbiConfig getConfigByType(String type) {

        for (DellEbiConfig config : configs) {
            if (config.getType().equalsIgnoreCase(type)) {
                return config;
            }
        }

        throw new LedpException(LedpCode.LEDP_29000, new String[] { type });
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

        return config.getIsDeleted();
    }

    @Override
    public Boolean getIsActive(String type) {

        if (type == null) {
            throw new LedpException(LedpCode.LEDP_29001);
        }

        DellEbiConfig config = getConfigByType(type);

        return config.getIsActive();
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
    public String getQuartzJob(String type) {
        return null;
    }

}
