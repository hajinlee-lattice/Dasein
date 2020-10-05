package com.latticeengines.datacloud.match.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.dao.VboUsageReportDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.VboUsageReport;

@Component("vboUsageReportDao")
public class VboUsageReportDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<VboUsageReport>
        implements VboUsageReportDao {

    @Override
    protected Class<VboUsageReport> getEntityClass() {
        return VboUsageReport.class;
    }

}
