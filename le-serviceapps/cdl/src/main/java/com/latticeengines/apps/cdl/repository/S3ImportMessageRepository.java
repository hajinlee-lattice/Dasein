package com.latticeengines.apps.cdl.repository;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.S3ImportMessage;

public interface S3ImportMessageRepository extends BaseJpaRepository<S3ImportMessage, Long> {

}
