package com.latticeengines.apps.cdl.tray.repository;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTest;

public interface TrayConnectorTestRepository extends BaseJpaRepository<TrayConnectorTest, Long> {

    TrayConnectorTest findByWorkflowRequestId(String workflowRequestId);

}
