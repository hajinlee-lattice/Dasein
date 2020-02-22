package com.latticeengines.objectapi.service;

import com.latticeengines.domain.exposed.query.ConcreteRestriction;

public interface TempListService {

    String createTempListIfNotExists(ConcreteRestriction restriction, String redshiftPartition);

    void dropTempList(String tempTableName);

}
