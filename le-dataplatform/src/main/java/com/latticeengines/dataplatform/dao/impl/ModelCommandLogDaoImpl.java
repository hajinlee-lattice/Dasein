package com.latticeengines.dataplatform.dao.impl;

import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.ModelCommandLogDao;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandLog;

@Component("modelCommandLogDao")
public class ModelCommandLogDaoImpl extends BaseDaoImpl<ModelCommandLog> implements ModelCommandLogDao {

    @Autowired
    protected SessionFactory sessionFactoryDlOrchestration;

    @Override
    protected SessionFactory getSessionFactory() {        
        return sessionFactoryDlOrchestration;
    }
    
    public ModelCommandLogDaoImpl() {
        super();
    }

    @Override
    protected Class<ModelCommandLog> getEntityClass() {
        return ModelCommandLog.class;
    }

}