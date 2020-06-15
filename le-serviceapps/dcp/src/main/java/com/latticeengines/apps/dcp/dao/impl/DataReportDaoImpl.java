package com.latticeengines.apps.dcp.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.dcp.dao.DataReportDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;

@Component("dataReportDao")
public class DataReportDaoImpl extends BaseDaoImpl<DataReportRecord> implements DataReportDao {
    @Override
    protected Class<DataReportRecord> getEntityClass() {
        return DataReportRecord.class;
    }
}
