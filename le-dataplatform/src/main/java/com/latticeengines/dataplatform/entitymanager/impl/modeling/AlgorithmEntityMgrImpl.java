package com.latticeengines.dataplatform.entitymanager.impl.modeling;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.BaseDao;
import com.latticeengines.dataplatform.dao.modeling.AlgorithmDao;
import com.latticeengines.dataplatform.entitymanager.impl.BaseEntityMgrImpl;
import com.latticeengines.dataplatform.entitymanager.modeling.AlgorithmEntityMgr;
import com.latticeengines.domain.exposed.modeling.algorithm.AlgorithmBase;

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
