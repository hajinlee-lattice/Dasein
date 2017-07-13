package com.latticeengines.dellebi.entitymanager.impl;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class DellEbiConfigEntityMgrImpl extends BaseEntityMgrImpl<DellEbiConfig> implements DellEbiConfigEntityMgr {

    @Autowired
    private DellEbiConfigDao dellEbiConfigDao;

    private List<DellEbiConfig> configs;

    private static final Logger log = LoggerFactory.getLogger(DellEbiConfigEntityMgrImpl.class);

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

    private List<DellEbiConfig> getConfigsByQuartzJob(String quartzJob) {
        List<DellEbiConfig> subConfigs = new ArrayList<>();
        for (DellEbiConfig config : configs) {
            if (config.getQuartzJob().equalsIgnoreCase(quartzJob)) {
                subConfigs.add(config);
            }
        }

        if (subConfigs.size() == 0) {
            throw new LedpException(LedpCode.LEDP_29005, new String[] { quartzJob });
        }

        return subConfigs;
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

    @Override
    public String getFileTypesByQuartzJob(String quartzJob) {
        List<String> strs = new ArrayList<>();
        for (DellEbiConfig config : getConfigsByQuartzJob(quartzJob)) {
            strs.add(config.getType());
        }

        StringBuilder sb = new StringBuilder();
        for (String str : strs) {
            sb.append(str);
            sb.append(",");
        }

        return sb.substring(0, sb.length()-1).toString();
    }

}
