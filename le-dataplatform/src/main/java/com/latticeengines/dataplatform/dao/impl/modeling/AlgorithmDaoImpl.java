package com.latticeengines.dataplatform.dao.impl.modeling;

import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.modeling.AlgorithmDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modeling.algorithm.AlgorithmBase;

@Component("algorithmDao")
public class AlgorithmDaoImpl extends BaseDaoImpl<AlgorithmBase> implements AlgorithmDao {


    public AlgorithmDaoImpl() {
        super();
    }

    protected Class<AlgorithmBase> getEntityClass() {
        return AlgorithmBase.class;
    }

}
