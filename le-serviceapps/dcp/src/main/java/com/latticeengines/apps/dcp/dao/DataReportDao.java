package com.latticeengines.apps.dcp.dao;

import java.util.Set;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;

public interface DataReportDao extends BaseDao<DataReportRecord> {
    void deleteDataReportRecords(Set<Long> pids);
}
