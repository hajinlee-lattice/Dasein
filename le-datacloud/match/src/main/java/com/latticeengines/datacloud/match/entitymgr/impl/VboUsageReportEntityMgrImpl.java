package com.latticeengines.datacloud.match.entitymgr.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.datacloud.match.dao.VboUsageReportDao;
import com.latticeengines.datacloud.match.entitymgr.VboUsageReportEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.VboUsageReport;

@Component("vboUsageReportEntityMgr")
public class VboUsageReportEntityMgrImpl implements VboUsageReportEntityMgr {

    @Inject
    private VboUsageReportDao vboUsageReportDao;

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW)
    public VboUsageReport create(VboUsageReport report) {
        vboUsageReportDao.createOrUpdate(report);
        return vboUsageReportDao.findByKey(report);
    }

}
