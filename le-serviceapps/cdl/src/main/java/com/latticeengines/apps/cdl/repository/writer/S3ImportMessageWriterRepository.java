package com.latticeengines.apps.cdl.repository.writer;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.repository.S3ImportMessageRepository;

public interface S3ImportMessageWriterRepository extends S3ImportMessageRepository {

    @Transactional(transactionManager = "jpaTransactionManager")
    @Modifying(clearAutomatically = true)
    @Query("UPDATE S3ImportMessage m SET m.hostUrl = ?2 WHERE m.key = ?1")
    void updateHostUrl(String key, String hostUrl);

}
