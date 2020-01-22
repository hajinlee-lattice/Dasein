package com.latticeengines.dataplatform.entitymanager.impl.modeling;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.modeling.AlgorithmDao;
import com.latticeengines.dataplatform.entitymanager.modeling.AlgorithmEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.modeling.algorithm.AlgorithmBase;

@Component("algorithmEntityMgr")
public class AlgorithmEntityMgrImpl extends BaseEntityMgrImpl<AlgorithmBase> implements AlgorithmEntityMgr {

    @Inject
    private AlgorithmDao algorithmDao;
     
    public AlgorithmEntityMgrImpl() {
        super();
    }   

    @Override 
    public BaseDao<AlgorithmBase> getDao() {
        return algorithmDao;
    }  

}
