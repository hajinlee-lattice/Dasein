package com.latticeengines.dellebi.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.dellebi.dao.DellEbiConfigDao;
import com.latticeengines.domain.exposed.dellebi.DellEbiConfig;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

//@Repository
@Component("dellEbiConfigDao")
public class DellEbiConfigDaoImpl extends BaseDaoImpl<DellEbiConfig>implements DellEbiConfigDao {

    private static List<DellEbiConfig> configs;

    public DellEbiConfigDaoImpl() {
        super();
    }

    @Override
    protected Class<DellEbiConfig> getEntityClass() {
        return DellEbiConfig.class;
    }

    @Override
    @SuppressWarnings({ "unchecked" })
    public DellEbiConfig getConfig(String type) {

        Session session = getSessionFactory().getCurrentSession();

        Class<DellEbiConfig> entityClz = getEntityClass();
        String queryStr = String.format("from %s where Type = :type", entityClz.getSimpleName());

        Query query = session.createQuery(queryStr);
        query.setString("type", type);
        List<DellEbiConfig> list = query.list();
        if (list.size() == 0) {
            throw new LedpException(LedpCode.LEDP_29000, new String[] { type });
        }

        return list.get(0);
    }

    @Override
    @SuppressWarnings({ "unchecked" })
    public List<DellEbiConfig> queryConfigs() {

        Session session = getSessionFactory().getCurrentSession();

        Class<DellEbiConfig> entityClz = getEntityClass();
        String queryStr = String.format("from %s ", entityClz.getSimpleName());

        Query query = session.createQuery(queryStr);
        List<DellEbiConfig> list = query.list();
        if (list.size() == 0) {
            throw new LedpException(LedpCode.LEDP_29001);
        }

        return list;
    }

    @Transactional(value = "transactionManagerDellEbiCfg", propagation = Propagation.REQUIRED)
    public void setConfigs() {
        List<DellEbiConfig> configlist = queryConfigs();
        if (configlist == null) {
            throw new LedpException(LedpCode.LEDP_29001);
        }
        configs = configlist;
    }

    public List<DellEbiConfig> getConfigs() {
        return configs;
    }

}
