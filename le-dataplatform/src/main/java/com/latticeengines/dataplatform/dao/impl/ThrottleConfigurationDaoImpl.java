package com.latticeengines.dataplatform.dao.impl;

import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.latticeengines.dataplatform.dao.ThrottleConfigurationDao;
import com.latticeengines.domain.exposed.dataplatform.ThrottleConfiguration;

@Repository("throttleConfigurationDao")
public class ThrottleConfigurationDaoImpl extends BaseDaoImpl<ThrottleConfiguration> implements
        ThrottleConfigurationDao {

    @Autowired
    protected SessionFactory sessionFactory;

    @Override
    protected SessionFactory getSessionFactory() {        
        return sessionFactory;
    }
    
    @Override
    protected Class<ThrottleConfiguration> getEntityClass() {
        return ThrottleConfiguration.class;
    }
    

}
