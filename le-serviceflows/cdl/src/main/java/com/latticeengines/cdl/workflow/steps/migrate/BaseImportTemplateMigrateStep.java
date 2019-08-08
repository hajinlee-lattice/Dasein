package com.latticeengines.cdl.workflow.steps.migrate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.collections4.map.HashedMap;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.ImportTemplateMigrateStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.MigrateTrackingProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

public abstract class BaseImportTemplateMigrateStep<T extends ImportTemplateMigrateStepConfiguration>
        extends BaseWorkflowStep<T> {

    static final String TEMPLATE_PATTERN = "Migrated_%s_%s";
    static final String FEEDTYPE_PATTERN = "%s_%s";

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    protected CDLProxy cdlProxy;

    @Inject
    protected MigrateTrackingProxy migrateTrackingProxy;

    @Override
    public void execute() {
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        List<DataFeedTask> dataFeedTasks = dataFeedProxy.getDataFeedTaskByUniqueIds(customerSpace.toString(),
                configuration.getDataFeedTaskList());
        Table newTemplate = mergeTemplates(dataFeedTasks);
        S3ImportSystem importSystem = cdlProxy.getS3ImportSystem(customerSpace.toString(), getSystemName());
        updateTemplate(newTemplate, importSystem);
        metadataProxy.createImportTable(customerSpace.toString(), newTemplate.getName(), newTemplate);

        Optional<DataFeedTask> optionalDataFeedTask =
                dataFeedTasks.stream().filter(dataFeedTask -> dataFeedTask.getFeedType().equals(getFeedType())).findFirst();
        if (optionalDataFeedTask.isPresent()) {
            DataFeedTask dataFeedTask = optionalDataFeedTask.get();
            dataFeedTask.setImportTemplate(newTemplate);
            dataFeedProxy.updateDataFeedTask(customerSpace.toString(), dataFeedTask);
            updateMigrateTracking(dataFeedTask.getUniqueId(), newTemplate.getName());
        } else {
            String newTaskId = NamingUtils.uuid("DataFeedTask");
            DataFeedTask dataFeedTask = new DataFeedTask();
            dataFeedTask.setUniqueId(newTaskId);
            dataFeedTask.setImportTemplate(newTemplate);
            dataFeedTask.setStatus(DataFeedTask.Status.Active);
            dataFeedTask.setEntity(getEntity());
            dataFeedTask.setFeedType(getFeedType());
            dataFeedTask.setSource(SourceType.FILE.getName());
            dataFeedTask.setActiveJob("Not specified");
            dataFeedTask.setSourceConfig("Not specified");
            dataFeedTask.setStartTime(new Date());
            dataFeedTask.setLastImported(new Date(0L));
            dataFeedTask.setLastUpdated(new Date());
            dataFeedProxy.createDataFeedTask(customerSpace.toString(), dataFeedTask);
            updateMigrateTracking(newTaskId, newTemplate.getName());
        }
    }

    private Table mergeTemplates(List<DataFeedTask> dataFeedTasks) {
        Table template = TableUtils.clone(dataFeedTasks.get(0).getImportTemplate(), getTemplateName());
        Map<String, Attribute> templateAttrs =
                template.getAttributes().stream().collect(Collectors.toMap(Attribute::getName, attr -> attr));
        Map<String, Attribute> mergedAttrs = new HashedMap<>();
        for (int i = 1; i < dataFeedTasks.size(); i++) {
            Map<String, Attribute> newAttrs =
                    dataFeedTasks.get(i).getImportTemplate().getAttributes()
                            .stream()
                            .filter(attr -> !templateAttrs.containsKey(attr.getName())
                                    && !mergedAttrs.containsKey(attr.getName()))
                            .collect(Collectors.toMap(Attribute::getName, attr -> attr));
            mergedAttrs.putAll(newAttrs);
        }
        template.addAttributes(new ArrayList<>(mergedAttrs.values()));
        return template;
    }

    String getSystemName() {
        return getStringValueFromContext(PRIMARY_IMPORT_SYSTEM);
    }

    Attribute getCustomerAccountId(String displayName) {
        Attribute attribute = new Attribute();
        attribute.setName(InterfaceName.CustomerAccountId.name());
        attribute.setDisplayName(displayName);
        attribute.setInterfaceName(InterfaceName.CustomerAccountId);
        attribute.setAllowedDisplayNames(Arrays.asList("ACCOUNT", "ACCOUNT_ID", "ACCOUNTID","ACCOUNT_EXTERNAL_ID"));
        attribute.setPhysicalDataType(Schema.Type.STRING.toString());
        attribute.setLogicalDataType(LogicalDataType.Id);
        attribute.setFundamentalType(FundamentalType.ALPHA);
        attribute.setApprovedUsage(ModelingMetadata.NONE_APPROVED_USAGE);
        return attribute;
    }

    Attribute getCustomerContactId(String displayName) {
        Attribute attribute = new Attribute();
        attribute.setName(InterfaceName.CustomerContactId.name());
        attribute.setDisplayName(displayName);
        attribute.setInterfaceName(InterfaceName.CustomerContactId);
        attribute.setAllowedDisplayNames(Arrays.asList("CONTACT", "CONTACTID", "CONTACT_EXTERNAL_ID", "CONTACT_ID"));
        attribute.setPhysicalDataType(Schema.Type.STRING.toString());
        attribute.setLogicalDataType(LogicalDataType.Id);
        attribute.setFundamentalType(FundamentalType.ALPHA);
        attribute.setApprovedUsage(ModelingMetadata.NONE_APPROVED_USAGE);
        return attribute;
    }

    Attribute getSystemId(String attrName, String displayName) {
        Attribute attribute = new Attribute();
        attribute.setName(attrName);
        attribute.setDisplayName(displayName);
        attribute.setPhysicalDataType(Schema.Type.STRING.toString());
        attribute.setLogicalDataType(LogicalDataType.Id);
        attribute.setFundamentalType(FundamentalType.ALPHA);
        attribute.setApprovedUsage(ModelingMetadata.NONE_APPROVED_USAGE);
        return attribute;
    }

    protected abstract String getTemplateName();

    protected abstract String getEntity();

    protected abstract String getFeedType();

    protected abstract void updateTemplate(Table templateTable, S3ImportSystem s3ImportSystem);

    protected abstract void updateMigrateTracking(String taskId, String templateName);
}
