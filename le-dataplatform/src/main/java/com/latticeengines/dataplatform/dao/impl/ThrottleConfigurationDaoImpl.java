package com.latticeengines.dataplatform.dao.impl;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dataplatform.dao.ThrottleConfigurationDao;
import com.latticeengines.domain.exposed.dataplatform.ThrottleConfiguration;

@Repository("throttleConfigurationDao")
public class ThrottleConfigurationDaoImpl extends BaseDaoImpl<ThrottleConfiguration> implements
        ThrottleConfigurationDao {

    @Override
    protected Class<ThrottleConfiguration> getEntityClass() {
        return ThrottleConfiguration.class;
    }
    
    @Override
    @Transactional(propagation = Propagation.REQUIRED)    
    public void deleteAll() {
        Session session = sessionFactory.getCurrentSession();
        Class<ThrottleConfiguration> entityClz = getEntityClass();        
        Query query = session.createQuery("delete from "+entityClz.getSimpleName());        
        query.executeUpdate(); 
    }

}
