package com.latticeengines.apps.cdl.service.impl;

import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataOperationEntityMgr;
import com.latticeengines.apps.cdl.service.DataOperationService;
import com.latticeengines.apps.core.service.DropBoxService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.DataDeleteOperationConfiguration;
import com.latticeengines.domain.exposed.cdl.DataOperationConfiguration;
import com.latticeengines.domain.exposed.cdl.DataOperationRequest;
import com.latticeengines.domain.exposed.cdl.DeleteRequest;
import com.latticeengines.domain.exposed.metadata.DataOperation;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.pls.FileProperty;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.lp.SourceFileProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("dataOperationService")
public class DataOperationServiceImpl implements DataOperationService {

    private static final Logger log = LoggerFactory.getLogger(DataOperationServiceImpl.class);

    private static final String FULL_PATH_PATTERN = "%s/%s/%s";

    private static final String DATA_OPERATION_PATH_PATTERN = "Data_Operation/%s_By_%s_%s/";

    private static final String DEFAULTSYSTEM = "DefaultSystem";

    @Inject
    private DataOperationEntityMgr dataOperationEntityMgr;

    @Inject
    private DropBoxService dropBoxService;

    @Inject
    private SourceFileProxy sourceFileProxy;

    @Inject
    private CDLDataCleanupServiceImpl cdlDataCleanupService;

    @Inject
    private BatonService batonService;

    @Inject
    private MetadataProxy metadataProxy;

    @Override
    public String createDataOperation(String customerSpace, DataOperation.OperationType operationType, DataOperationConfiguration configuration) {
        DataOperation dataOperation = new DataOperation();
        dataOperation.setOperationType(operationType);
        dataOperation.setConfiguration(configuration);
        dataOperation.setTenant(MultiTenantContext.getTenant());
        String dropPath = generateDropPath(dataOperation);
        String fullPath = String.format(FULL_PATH_PATTERN, dropBoxService.getDropBoxBucket(), dropBoxService.getDropBoxPrefix(),
                dropPath);
        DataOperation existing = findDataOperationByDropPath(customerSpace, fullPath);
        if (existing != null) {
            return fullPath;
        }
        dropBoxService.createFolderUnderDropFolder(dropPath);
        dataOperation.setDropPath(fullPath);
        dataOperationEntityMgr.create(dataOperation);
        return fullPath;
    }

    public DataOperation findDataOperationByDropPath(String customerSpace, String dropPath) {
        return dataOperationEntityMgr.findByDropPath(dropPath);
    }

    private String generateDropPath(DataOperation dataOperation) {
        String idColumn = BusinessEntity.Account.equals(dataOperation.getConfiguration().getEntity()) ? InterfaceName.AccountId.name()
                : InterfaceName.ContactId.name();
        String systemName = StringUtils.isEmpty(dataOperation.getConfiguration().getSystemName()) ? DEFAULTSYSTEM
                : dataOperation.getConfiguration().getSystemName();
        return String.format(DATA_OPERATION_PATH_PATTERN, dataOperation.getOperationType(), systemName, idColumn);
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

    @Override
    public ApplicationId submitJob(String customerSpace, DataOperationRequest dataOperationRequest) {
        try {
            DataOperation dataOperation = findDataOperationByDropPath(customerSpace, dataOperationRequest.getS3DropPath());
            String key = dataOperationRequest.getS3FileKey();
            String fileName = key.substring(key.lastIndexOf("/") + 1);
            FileProperty fileProperty = new FileProperty();
            fileProperty.setFileName(fileName);
            fileProperty.setFilePath(dataOperationRequest.getS3Bucket() + "/" + key);
            fileProperty.setDirectory(false);
            SchemaInterpretation schema = Account.equals(dataOperation.getConfiguration().getEntity()) ? SchemaInterpretation.DeleteByAccountTemplate
                    : SchemaInterpretation.DeleteByContactTemplate;
            SourceFile sourceFile = sourceFileProxy.createSourceFileFromS3(customerSpace, fileProperty, dataOperation.getConfiguration().getEntity().name(),
                    schema.name());
            resolveMetadata(customerSpace, sourceFile);
            DataOperationConfiguration configuration = dataOperation.getConfiguration();
            if (configuration instanceof DataDeleteOperationConfiguration) {
                DataDeleteOperationConfiguration deleteOperationConfiguration = (DataDeleteOperationConfiguration) configuration;
                DeleteRequest request = new DeleteRequest();
                request.setIdEntity(deleteOperationConfiguration.getEntity());
                request.setFilename(sourceFile.getName());
                request.setHardDelete(DataDeleteOperationConfiguration.DeleteType.HARD.equals(deleteOperationConfiguration.getDeleteType()));
                request.setIdSystem(dataOperation.getConfiguration().getSystemName());
                customerSpace = CustomerSpace.parse(customerSpace).toString();
                return cdlDataCleanupService.registerDeleteData(customerSpace, request);
            } else {
                throw new UnsupportedOperationException("Unsupported data operation");
            }
        } catch (Exception e) {
            log.error("error:", e);
            throw e;
        }
    }

    private void resolveMetadata(String customerSpace, SourceFile sourceFile) {
        log.info("Resolving metadata for modeling ...");
        Table table = SchemaRepository.instance().getSchema(sourceFile.getSchemaInterpretation(), false,
                batonService.isEntityMatchEnabled(CustomerSpace.parse(customerSpace)),
                batonService.onlyEntityMatchGAEnabled(CustomerSpace.parse(customerSpace)));
        table.setName("SourceFile_" + sourceFile.getName().replace(".", "_"));
        metadataProxy.createTable(customerSpace, table.getName(), table);
        sourceFile.setTableName(table.getName());
        sourceFileProxy.update(customerSpace, sourceFile);
    }
}
