package com.latticeengines.metadata.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;

public interface SegmentEntityMgr extends BaseEntityMgr<MetadataSegment> {

    MetadataSegment findByName(String name);
 
}
