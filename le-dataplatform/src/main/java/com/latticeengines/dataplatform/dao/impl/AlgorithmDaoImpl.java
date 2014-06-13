package com.latticeengines.dataplatform.dao.impl;

import org.springframework.stereotype.Repository;

import com.latticeengines.dataplatform.dao.AlgorithmDao;
import com.latticeengines.domain.exposed.dataplatform.Algorithm;
import com.latticeengines.domain.exposed.dataplatform.algorithm.AlgorithmBase;

@Repository("algorithmDao")
public class AlgorithmDaoImpl extends BaseDaoImpl<AlgorithmBase> implements AlgorithmDao {

  
    public AlgorithmDaoImpl() {
        super();
    }

    protected Class<AlgorithmBase> getEntityClass() {
        return AlgorithmBase.class;
    }
  
}
