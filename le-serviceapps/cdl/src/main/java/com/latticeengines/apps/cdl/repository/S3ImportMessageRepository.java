package com.latticeengines.apps.cdl.repository;

import java.util.List;

import org.springframework.data.jpa.repository.Query;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.S3ImportMessage;
import com.latticeengines.domain.exposed.jms.S3ImportMessageType;

public interface S3ImportMessageRepository extends BaseJpaRepository<S3ImportMessage, Long> {

    @Query("SELECT m FROM S3ImportMessage m WHERE m.hostUrl IS NOT NULL AND m.pid IN " +
            "(SELECT MIN(sm.pid) FROM S3ImportMessage sm GROUP BY sm.dropBox, sm.feedType)")
    List<S3ImportMessage> getS3ImportMessageGroupByDropBox();

    S3ImportMessage findByKey(String key);

    List<S3ImportMessage> findByMessageTypeAndHostUrlIsNull(S3ImportMessageType messageType);

}
