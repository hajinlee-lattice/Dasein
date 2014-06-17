package com.latticeengines.dataplatform.dao.impl;

import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.latticeengines.dataplatform.dao.AlgorithmDao;
import com.latticeengines.domain.exposed.dataplatform.algorithm.AlgorithmBase;

@Repository("algorithmDao")
public class AlgorithmDaoImpl extends BaseDaoImpl<AlgorithmBase> implements AlgorithmDao {

    @Autowired
    protected SessionFactory sessionFactory;

    @Override
    protected SessionFactory getSessionFactory() {        
        return sessionFactory;
    }
    
    public AlgorithmDaoImpl() {
        super();
    }

    protected Class<AlgorithmBase> getEntityClass() {
        return AlgorithmBase.class;
    }
  
}
