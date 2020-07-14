package com.latticeengines.datacloud.match.repository.reader;

import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockDomainEntitlement;

@Transactional(readOnly = true, propagation = Propagation.REQUIRES_NEW)
public interface DataBlockDomainEntitlementRepository extends BaseJpaRepository<DataBlockDomainEntitlement, Long> {

}
