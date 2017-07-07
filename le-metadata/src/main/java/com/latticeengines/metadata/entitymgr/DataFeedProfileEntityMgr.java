package com.latticeengines.metadata.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedProfile;

public interface DataFeedProfileEntityMgr extends BaseEntityMgr<DataFeedProfile> {

    DataFeedProfile findByProfileId(Long profileId);

}
