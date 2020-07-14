package com.latticeengines.datacloud.match.repository.reader;

import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockLevelMetadata;

@Transactional(readOnly = true, propagation = Propagation.REQUIRES_NEW)
public interface DataBlockLevelMetadataRepository extends BaseJpaRepository<DataBlockLevelMetadata, Long> {

}
