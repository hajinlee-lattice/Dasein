package com.latticeengines.apps.dcp.dao.impl;

import org.springframework.stereotype.Service;

import com.latticeengines.apps.dcp.dao.UploadStatisticsDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.dcp.UploadStatsContainer;


@Service
public class UploadStatisticsDaoImpl extends BaseDaoImpl<UploadStatsContainer> implements UploadStatisticsDao {

    @Override
    protected Class<UploadStatsContainer> getEntityClass() {
        return UploadStatsContainer.class;
    }

}
