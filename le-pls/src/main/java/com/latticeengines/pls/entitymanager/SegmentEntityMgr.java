package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.Segment;

public interface SegmentEntityMgr extends BaseEntityMgr<Segment> {

    List<Segment> getAll();

}
