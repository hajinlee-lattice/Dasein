package com.latticeengines.dataplatform.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.AlgorithmDao;
import com.latticeengines.dataplatform.dao.BaseDao;
import com.latticeengines.dataplatform.entitymanager.AlgorithmEntityMgr;
import com.latticeengines.domain.exposed.dataplatform.algorithm.AlgorithmBase;

@Component("algorithmEntityMgr")
public class AlgorithmEntityMgrImpl extends BaseEntityMgrImpl<AlgorithmBase> implements AlgorithmEntityMgr {

    @Autowired
    private AlgorithmDao algorithmDao;
     
    public AlgorithmEntityMgrImpl() {
        super();
    }   

    @Override 
    public BaseDao<AlgorithmBase> getDao() {
        return algorithmDao;
    }  

}
