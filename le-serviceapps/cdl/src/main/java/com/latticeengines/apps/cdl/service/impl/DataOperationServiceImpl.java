package com.latticeengines.apps.cdl.service.impl;

import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataOperationEntityMgr;
import com.latticeengines.apps.cdl.service.DataOperationService;
import com.latticeengines.apps.core.service.DropBoxService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.DataOperationConfiguration;
import com.latticeengines.domain.exposed.metadata.DataOperation;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("dataOperationService")
public class DataOperationServiceImpl implements DataOperationService {

    private static final Logger log = LoggerFactory.getLogger(SegmentServiceImpl.class);

    private static final String FULL_PATH_PATTERN = "%s/%s/%s";

    private static final String DATA_OPERATION_PATH_PATTERN = "Data_Operation/%s_By_%s_%s/";

    @Inject
    private DataOperationEntityMgr dataOperationEntityMgr;

    @Inject
    private DropBoxService dropBoxService;

    @Override
    public String createDataOperation(String customerSpace, DataOperation.OperationType operationType, DataOperationConfiguration configuration) {
        DataOperation dataOperation = new DataOperation();
        dataOperation.setOperationType(operationType);
        dataOperation.setConfiguration(configuration);
        dataOperation.setTenant(MultiTenantContext.getTenant());
        dataOperation.setCreateDate(new Date());
        dataOperation.setDropPath(generateDropPath(dataOperation));
        dataOperationEntityMgr.create(dataOperation);
        return dataOperation.getDropPath();
    }

    private String generateDropPath(DataOperation dataOperation) {
        String idColumn = BusinessEntity.Account.equals(dataOperation.getConfiguration().getEntity()) ? InterfaceName.AccountId.name()
                : InterfaceName.ContactId.name();
        String dataOperationPath = String.format(DATA_OPERATION_PATH_PATTERN, dataOperation.getOperationType(),
                dataOperation.getConfiguration().getSystemName(), idColumn);
        dropBoxService.createFolderUnderDropFolder(dataOperationPath);
        return String.format(FULL_PATH_PATTERN, dropBoxService.getDropBoxBucket(), dropBoxService.getDropBoxPrefix(),
                dataOperationPath);
    }

    @Override
    public void updateDataOperation(String customerSpace, DataOperation dataOperation) {
        dataOperationEntityMgr.update(dataOperation);
    }

    @Override
    public List<DataOperation> findAllDataOperation(String customerSpace) {
        return dataOperationEntityMgr.findAll();
    }

    @Override
    public void deleteDataOperation(String customerSpace,DataOperation dataOperation) {
        dataOperationEntityMgr.delete(dataOperation);
    }
}
