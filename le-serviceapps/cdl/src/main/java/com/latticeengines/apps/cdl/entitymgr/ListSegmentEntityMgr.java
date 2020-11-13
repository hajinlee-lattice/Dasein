package com.latticeengines.apps.cdl.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.metadata.ListSegment;

public interface ListSegmentEntityMgr extends BaseEntityMgrRepository<ListSegment, Long> {

    ListSegment updateListSegment(ListSegment incomingListSegment);

    ListSegment findByExternalInfo(String externalSystem, String externalSegmentId);
}
