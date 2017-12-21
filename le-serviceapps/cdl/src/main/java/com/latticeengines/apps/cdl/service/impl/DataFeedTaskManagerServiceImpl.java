package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.cdl.service.DLTenantMappingService;
import com.latticeengines.apps.cdl.service.DataFeedMetadataService;
import com.latticeengines.apps.cdl.service.DataFeedTaskManagerService;
import com.latticeengines.apps.cdl.workflow.CDLDataFeedImportWorkflowSubmitter;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLImportConfig;
import com.latticeengines.domain.exposed.cdl.CSVImportFileInfo;
import com.latticeengines.domain.exposed.dataloader.DLTenantMapping;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("dataFeedTaskManagerService")
public class DataFeedTaskManagerServiceImpl implements DataFeedTaskManagerService {

    private static final Logger log = LoggerFactory.getLogger(DataFeedTaskManagerServiceImpl.class);

    private final DataFeedProxy dataFeedProxy;

    private final TenantService tenantService;

    private final DLTenantMappingService dlTenantMappingService;

    private final CDLDataFeedImportWorkflowSubmitter cdlDataFeedImportWorkflowSubmitter;

    @Value("${cdl.dataloader.tenant.mapping.enabled:false}")
    private boolean dlTenantMappingEnabled;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceProxy;

    @PostConstruct
    public void init() {
        internalResourceProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    @Inject
    public DataFeedTaskManagerServiceImpl(CDLDataFeedImportWorkflowSubmitter cdlDataFeedImportWorkflowSubmitter,
            DataFeedProxy dataFeedProxy, TenantService tenantService, DLTenantMappingService dlTenantMappingService) {
        this.cdlDataFeedImportWorkflowSubmitter = cdlDataFeedImportWorkflowSubmitter;
        this.dataFeedProxy = dataFeedProxy;
        this.tenantService = tenantService;
        this.dlTenantMappingService = dlTenantMappingService;
    }

    @Override
    public String createDataFeedTask(String customerSpaceStr, String feedType, String entity, String source,
            CDLImportConfig importConfig) {
        DataFeedMetadataService dataFeedMetadataService = DataFeedMetadataService.getService(source);
        CustomerSpace customerSpace = dataFeedMetadataService.getCustomerSpace(importConfig);
        if (dlTenantMappingEnabled) {
            log.info("DL tenant mapping is enabled");
            customerSpace = mapCustomerSpace(customerSpace);
        }
        Tenant tenant = tenantService.findByTenantId(customerSpace.toString());
        if (tenant == null) {
            throw new RuntimeException(String.format("Cannot find the tenant %s", customerSpace.getTenantId()));
        }
        MultiTenantContext.setTenant(tenant);
        Table newMeta = dataFeedMetadataService.getMetadata(importConfig, entity);
        Table schemaTable = SchemaRepository.instance().getSchema(BusinessEntity.valueOf(entity));

        newMeta = dataFeedMetadataService.resolveMetadata(newMeta, schemaTable);
        setCategoryForTable(newMeta, entity);
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source, feedType, entity);
        if (dataFeedTask != null) {
            crosscheckDataType(customerSpace, entity, source, newMeta, dataFeedTask.getUniqueId());
            Table originMeta = dataFeedTask.getImportTemplate();
            DataFeed dataFeed = dataFeedProxy.getDataFeed(customerSpace.toString());
            if (!dataFeedMetadataService.compareMetadata(originMeta, newMeta,
                    !dataFeed.getStatus().equals(DataFeed.Status.Initing))) {
                dataFeedTask.setStatus(DataFeedTask.Status.Updated);
                dataFeedTask.setImportTemplate(newMeta);
                dataFeedProxy.updateDataFeedTask(customerSpace.toString(), dataFeedTask);
            }
            return dataFeedTask.getUniqueId();
        } else {
            crosscheckDataType(customerSpace, entity, source, newMeta, "");
            dataFeedTask = new DataFeedTask();
            dataFeedTask.setUniqueId(NamingUtils.uuid("DataFeedTask"));
            dataFeedTask.setImportTemplate(newMeta);
            dataFeedTask.setStatus(DataFeedTask.Status.Active);
            dataFeedTask.setEntity(entity);
            dataFeedTask.setFeedType(feedType);
            dataFeedTask.setSource(source);
            dataFeedTask.setActiveJob("Not specified");
            dataFeedTask.setSourceConfig("Not specified");
            dataFeedTask.setStartTime(new Date());
            dataFeedTask.setLastImported(new Date(0L));
            dataFeedProxy.createDataFeedTask(customerSpace.toString(), dataFeedTask);
            if (dataFeedMetadataService.needUpdateDataFeedStatus()) {
                DataFeed dataFeed = dataFeedProxy.getDataFeed(customerSpace.toString());
                if (dataFeed.getStatus().equals(DataFeed.Status.Initing)) {
                    dataFeedProxy.updateDataFeedStatus(customerSpace.toString(), DataFeed.Status.Initialized.getName());
                }
            }
            return dataFeedTask.getUniqueId();
        }
    }

    private void setCategoryForTable(Table table, String entity) {
        BusinessEntity businessEntity = BusinessEntity.valueOf(entity);
        if (businessEntity == null) {
            throw new RuntimeException(String.format("Cannot recognize entity: %s", entity));
        }
        String category;
        switch (businessEntity) {
        case Account:
            category = Category.ACCOUNT_ATTRIBUTES.name();
            break;
        case Contact:
            category = Category.CONTACT_ATTRIBUTES.name();
            break;
        // todo other entity
        default:
            category = Category.DEFAULT.getName();
        }
        for (Attribute attr : table.getAttributes()) {
            attr.setCategory(category);
        }
    }

    @Override
    public String submitImportJob(String customerSpaceStr, String taskIdentifier, CDLImportConfig importConfig) {
        CustomerSpace customerSpace = CustomerSpace.parse(customerSpaceStr);
        if (dlTenantMappingEnabled) {
            customerSpace = mapCustomerSpace(customerSpace);
        }
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), taskIdentifier);
        if (dataFeedTask == null) {
            throw new RuntimeException("Cannot find the data feed task!");
        }
        DataFeedMetadataService dataFeedMetadataService = DataFeedMetadataService.getService(dataFeedTask.getSource());
        String connectorConfig = dataFeedMetadataService.getConnectorConfig(importConfig, dataFeedTask.getUniqueId());
        CSVImportFileInfo csvImportFileInfo = dataFeedMetadataService.getImportFileInfo(importConfig);
        log.info(String.format("csvImportFileInfo=%s", csvImportFileInfo));
        ApplicationId appId = cdlDataFeedImportWorkflowSubmitter.submit(customerSpace, dataFeedTask, connectorConfig,
                csvImportFileInfo);
        registerImportAction(customerSpaceStr, appId, csvImportFileInfo);
        return appId.toString();
    }

    @VisibleForTesting
    Action registerImportAction(String customerSpaceStr, @NonNull ApplicationId appId,
            CSVImportFileInfo csvImportFileInfo) {
        Action action = new Action();
        action.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        action.setActionInitiator(csvImportFileInfo.getFileUploadInitiator());
        Tenant tenant = tenantService.findByTenantId(customerSpaceStr);
        if (tenant == null) {
            throw new NullPointerException(String.format("Tenant is null with id %s", customerSpaceStr));
        }
        action.setTenant(tenant);
        action.setTrackingId(appId.toString());

        return internalResourceProxy.createAction(customerSpaceStr, action);
    }

    private CustomerSpace mapCustomerSpace(CustomerSpace customerSpace) {
        CustomerSpace newCustomerSpace = customerSpace;
        String dlTenantId = customerSpace.getTenantId();
        DLTenantMapping dlTenantMapping = dlTenantMappingService.getDLTenantMapping(dlTenantId, "*");
        if (dlTenantMapping != null) {
            newCustomerSpace = CustomerSpace.parse(dlTenantMapping.getTenantId());
        }
        log.info(String.format("original tenant %s, new tenant %s", customerSpace.getTenantId(),
                newCustomerSpace.getTenantId()));
        return newCustomerSpace;
    }

    private void crosscheckDataType(CustomerSpace customerSpace, String entity, String source, Table metaTable,
            String dataFeedTaskUniqueId) {
        List<DataFeedTask> dataFeedTasks = dataFeedProxy.getDataFeedTaskWithSameEntity(customerSpace.toString(),
                entity);
        if (dataFeedTasks == null || dataFeedTasks.size() == 0) {
            return;
        } else {
            for (DataFeedTask dataFeedTask : dataFeedTasks) {
                if (StringUtils.equals(dataFeedTask.getUniqueId(), dataFeedTaskUniqueId)) {
                    continue;
                }
                List<String> inconsistentAttrs = compareAttribute(dataFeedTask.getSource(),
                        dataFeedTask.getImportTemplate(), source, metaTable);
                if (inconsistentAttrs != null && inconsistentAttrs.size() > 0) {
                    throw new RuntimeException(String.format(
                            "The following field data type is not consistent with " + "the one that already exists: %s",
                            String.join(",", inconsistentAttrs)));
                }
            }
        }
    }

    private List<String> compareAttribute(String baseSource, Table baseTable, String targetSource, Table targetTable) {
        List<String> inconsistentAttrs = new ArrayList<>();
        DataFeedMetadataService baseService = DataFeedMetadataService.getService(baseSource);
        DataFeedMetadataService targetService = DataFeedMetadataService.getService(targetSource);
        Map<String, Attribute> baseAttrs = new HashMap<>();
        baseTable.getAttributes().forEach(attribute -> baseAttrs.put(attribute.getName().toLowerCase(), attribute));
        for (Attribute attr : targetTable.getAttributes()) {
            if (baseAttrs.containsKey(attr.getName().toLowerCase())) {
                Schema.Type baseType = baseService.getAvroType(baseAttrs.get(attr.getName().toLowerCase()));
                Schema.Type targetType = targetService.getAvroType(attr);
                if (baseType != targetType) {
                    inconsistentAttrs.add(attr.getName());
                }
            }
        }
        return inconsistentAttrs;
    }

    @VisibleForTesting
    void setInternalResourceRestApiProxy(InternalResourceRestApiProxy internalResourceRestApiProxy) {
        this.internalResourceProxy = internalResourceRestApiProxy;
    }
}
