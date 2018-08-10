package com.latticeengines.apps.cdl.repository;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobDetail;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobStatus;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;

public interface CDLJobDetailRepository extends BaseJpaRepository<CDLJobDetail, Long> {

    List<CDLJobDetail> findByCdlJobTypeAndCdlJobStatusOrderByPidDesc(CDLJobType cdlJobType, CDLJobStatus cdlJobStatus);

    CDLJobDetail findTopByCdlJobTypeOrderByPidDesc(CDLJobType cdlJobType);
}
