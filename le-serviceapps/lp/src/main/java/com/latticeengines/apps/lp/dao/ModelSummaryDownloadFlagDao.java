package com.latticeengines.apps.lp.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.ModelSummaryDownloadFlag;

public interface ModelSummaryDownloadFlagDao extends BaseDao<ModelSummaryDownloadFlag> {

    List<String> getWaitingFlags();

    List<String> getExcludeFlags();

    void deleteOldFlags(long timeTicks);

    void deleteExcludeFlag(String tenantId);
}
