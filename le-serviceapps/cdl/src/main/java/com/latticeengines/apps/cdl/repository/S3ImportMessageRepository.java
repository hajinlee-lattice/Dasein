package com.latticeengines.apps.cdl.repository;

import java.util.List;

import org.springframework.data.jpa.repository.Query;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.S3ImportMessage;

public interface S3ImportMessageRepository extends BaseJpaRepository<S3ImportMessage, Long> {

    @Query("select m from S3ImportMessage m where m.PID in " +
            "(select MIN(sm.PID) from S3ImportMessage sm group by sm.DropBox)")
    List<S3ImportMessage> getS3ImportMessageGroupByDropBox();

}
