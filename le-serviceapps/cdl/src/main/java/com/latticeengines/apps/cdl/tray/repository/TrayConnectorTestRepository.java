package com.latticeengines.apps.cdl.tray.repository;

import java.util.List;

import org.springframework.data.jpa.repository.Query;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTest;

public interface TrayConnectorTestRepository extends BaseJpaRepository<TrayConnectorTest, Long> {

    TrayConnectorTest findByWorkflowRequestId(String workflowRequestId);

    @Query("SELECT t FROM TrayConnectorTest t " + //
            "WHERE t.testResult IS NULL " + //
            "ORDER BY t.startTime ")
    List<TrayConnectorTest> findUnfinishedTests();

}
