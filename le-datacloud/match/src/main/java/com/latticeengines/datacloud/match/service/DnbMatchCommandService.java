package com.latticeengines.datacloud.match.service;

import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;
import com.latticeengines.domain.exposed.datacloud.manage.DnBMatchCommand;

public interface DnbMatchCommandService {

    void dnbMatchCommandCreate(DnBBatchMatchContext dnbMatchContext);
    void dnbMatchCommandUpdate(DnBBatchMatchContext dnbMatchContext);
    void dnbMatchCommandDelete(DnBMatchCommand record);
    DnBMatchCommand findRecordByField(String field, Object value);
}
