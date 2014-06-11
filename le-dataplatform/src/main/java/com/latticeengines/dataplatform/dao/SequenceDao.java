package com.latticeengines.dataplatform.dao;

import com.latticeengines.dataplatform.dao.impl.Sequence;

public interface SequenceDao   {

    Long nextVal(String key);
}
