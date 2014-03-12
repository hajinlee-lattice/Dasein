package com.latticeengines.dataplatform.dao;

import com.latticeengines.dataplatform.dao.impl.Sequence;

public interface SequenceDao extends BaseDao<Sequence> {

    Long nextVal(String key);
}
