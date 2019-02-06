package com.latticeengines.datacloud.match.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;
import com.latticeengines.domain.exposed.datacloud.manage.DnBMatchCommand;

public interface DnbMatchCommandService {
    void dnbMatchCommandCreate(DnBBatchMatchContext contexts);

    void dnbMatchCommandUpdate(DnBBatchMatchContext contexts);

    void dnbMatchCommandUpdateStatus(List<DnBBatchMatchContext> contexts);

    void dnbMatchCommandDelete(DnBMatchCommand record);

    DnBMatchCommand findRecordByField(String field, Object value);

    void finalize(String rootOperationUid);
}
