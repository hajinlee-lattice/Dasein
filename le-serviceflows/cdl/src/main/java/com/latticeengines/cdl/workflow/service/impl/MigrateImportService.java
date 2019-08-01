package com.latticeengines.cdl.workflow.service.impl;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.service.ConvertBatchStoreService;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.MigrateImportServiceConfiguration;

@Component("migrateImportService")
@Lazy(value = false)
public class MigrateImportService extends BaseConvertBatchStoreService<MigrateImportServiceConfiguration> implements ConvertBatchStoreService {
}
