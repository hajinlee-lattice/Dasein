package com.latticeengines.pls.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.ModelSummaryDownloadFlag;

import java.util.List;

public interface ModelSummaryDownloadFlagDao extends BaseDao<ModelSummaryDownloadFlag> {

    List<ModelSummaryDownloadFlag> getAllFlags();

    List<ModelSummaryDownloadFlag> getDownloadedFlags();

    List<ModelSummaryDownloadFlag> getWaitingFlags();
}
