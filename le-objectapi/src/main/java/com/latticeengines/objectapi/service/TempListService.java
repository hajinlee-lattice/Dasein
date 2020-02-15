package com.latticeengines.objectapi.service;

import com.latticeengines.domain.exposed.query.ConcreteRestriction;

public interface TempListService {

    String createTempListIfNotExists(ConcreteRestriction restriction);

    void dropTempList(String tempTableName);

}
