package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.Segment;

public interface PdSegmentEntityMgr extends BaseEntityMgr<Segment> {

    List<Segment> getAll();

    Segment findByName(String segmentName);

    Segment findByModelId(String modelId);

    void deleteByModelId(String modelId);

    Segment retrieveByModelIdForInternalOperations(String modelId);
}
