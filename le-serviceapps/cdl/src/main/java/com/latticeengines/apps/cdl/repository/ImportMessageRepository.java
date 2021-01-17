package com.latticeengines.apps.cdl.repository;

import org.springframework.data.repository.query.Param;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.ImportMessage;

public interface ImportMessageRepository extends BaseJpaRepository<ImportMessage, Long> {

    ImportMessage findBySourceId(@Param("sourceId") String sourceId);
}
