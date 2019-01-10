package com.latticeengines.apps.cdl.repository.writer;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;

public interface DataIntegrationStatusMonitoringWriterRepository
        extends BaseJpaRepository<DataIntegrationStatusMonitor, Long> {

}