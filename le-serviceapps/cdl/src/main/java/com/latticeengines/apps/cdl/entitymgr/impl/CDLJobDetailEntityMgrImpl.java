package com.latticeengines.apps.cdl.entitymgr.impl;

import java.time.Instant;
import java.util.Date;
import java.util.List;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.CDLJobDetailDao;
import com.latticeengines.apps.cdl.entitymgr.CDLJobDetailEntityMgr;
import com.latticeengines.apps.cdl.repository.CDLJobDetailRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobDetail;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobStatus;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;

@Component("cdlJobDetailEntityMgr")
public class CDLJobDetailEntityMgrImpl extends BaseReadWriteRepoEntityMgrImpl<CDLJobDetailRepository, CDLJobDetail, Long> implements
        CDLJobDetailEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(CDLJobDetailEntityMgrImpl.class);

    @Inject
    private CDLJobDetailDao cdlJobDetailDao;

    @Inject
    private CDLJobDetailEntityMgrImpl _self;

    @Resource(name = "CDLJobDetailReaderRepository")
    private CDLJobDetailRepository cdlJobDetailReaderRepository;

    @Resource(name = "CDLJobDetailWriterRepository")
    private CDLJobDetailRepository cdlJobDetailWriterRepository;

    @Override
    public BaseDao<CDLJobDetail> getDao() {
        return cdlJobDetailDao;
    }

    @Override
    protected CDLJobDetailRepository getReaderRepo() {
        return cdlJobDetailReaderRepository;
    }

    @Override
    protected CDLJobDetailRepository getWriterRepo() {
        return cdlJobDetailWriterRepository;
    }

    @Override
    protected CDLJobDetailEntityMgrImpl getSelf() {
        return _self;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<CDLJobDetail> listAllRunningJobByJobType(CDLJobType cdlJobType) {
        return cdlJobDetailReaderRepository.findByCdlJobTypeAndCdlJobStatusOrderByPidDesc(cdlJobType,
                CDLJobStatus.RUNNING);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public CDLJobDetail findLatestJobByJobType(CDLJobType cdlJobType) {
        return cdlJobDetailReaderRepository.findTopByCdlJobTypeOrderByPidDesc(cdlJobType);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public CDLJobDetail createJobDetail(CDLJobType cdlJobType, Tenant tenant) {
        CDLJobDetail cdlJobDetail = new CDLJobDetail();
        cdlJobDetail.setCdlJobType(cdlJobType);
        cdlJobDetail.setCdlJobStatus(CDLJobStatus.RUNNING);
        cdlJobDetail.setCreateDate(Date.from(Instant.now()));
        cdlJobDetail.setLastUpdateDate(Date.from(Instant.now()));
        cdlJobDetail.setTenant(tenant);
        cdlJobDetailDao.create(cdlJobDetail);
        return cdlJobDetail;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void updateJobDetail(CDLJobDetail cdlJobDetail) {
        log.debug(String.format("update cdl job detail %d", cdlJobDetail.getPid()));
        cdlJobDetail.setLastUpdateDate(Date.from(Instant.now()));
        cdlJobDetailDao.update(cdlJobDetail);
    }
}
