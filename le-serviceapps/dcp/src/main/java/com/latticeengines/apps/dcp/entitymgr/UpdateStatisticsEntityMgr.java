package com.latticeengines.apps.dcp.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadStatsContainer;

public interface UpdateStatisticsEntityMgr extends BaseEntityMgrRepository<UploadStatsContainer, Long> {

    UploadStatsContainer save(UploadStatsContainer container);

    UploadStatsContainer findByUploadAndId(Upload upload, Long statsId);

    UploadStatsContainer findIsLatest(Upload upload);

    UploadStatsContainer setAsLatest(UploadStatsContainer container);

}
