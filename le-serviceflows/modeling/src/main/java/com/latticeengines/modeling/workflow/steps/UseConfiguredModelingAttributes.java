package com.latticeengines.modeling.workflow.steps;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.UseConfiguredModelingAttributesConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("useConfiguredModelingAttributes")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class UseConfiguredModelingAttributes extends BaseWorkflowStep<UseConfiguredModelingAttributesConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(UseConfiguredModelingAttributes.class);

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    private DataCollection.Version dataCollectionVersion;

    private String customerSpace;

    private boolean excludeDataCloudAttributes;

    private boolean excludeCDLAttributes;

    @Override
    public void onConfigurationInitialized() {
        dataCollectionVersion = configuration.getDataCollectionVersion();
        if (dataCollectionVersion == null) {
            dataCollectionVersion = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
            log.info("Read inactive version from workflow context: " + dataCollectionVersion);
        } else {
            log.info("Use the version specified in configuration: " + dataCollectionVersion);
        }

        if (configuration.getCustomerSpace() == null) {
            throw new RuntimeException("Customer space is not found in configuration");
        }

        customerSpace = configuration.getCustomerSpace().toString();
        excludeDataCloudAttributes = configuration.isExcludeDataCloudAttributes();
        excludeCDLAttributes = configuration.isExcludeCDLAttributes();

        log.info(String.format("excludeDataCloudAttributes = %s, excludeCDLAttributes = %s", excludeDataCloudAttributes,
                excludeCDLAttributes));
    }

    @Override
    public void execute() {
        List<ColumnMetadata> userSelectedAttributesForModeling = servingStoreProxy
                .getNewModelingAttrs(customerSpace, dataCollectionVersion).collectList().block();
        log.info(String.format("userSelectedAttributesForModeling = %s",
                JsonUtils.serialize(userSelectedAttributesForModeling)));

        Table eventTable = getObjectFromContext(EVENT_TABLE, Table.class);
        log.info("Attributes from event table:" + JsonUtils.serialize(eventTable.getAttributes()));

        if (CollectionUtils.isNotEmpty(userSelectedAttributesForModeling)) {
            updateApprovedUsageForAttributes(userSelectedAttributesForModeling, eventTable);
            metadataProxy.updateTable(customerSpace, eventTable.getName(), eventTable);
            putObjectInContext(EVENT_TABLE, eventTable);
        } else {
            throw new RuntimeException("userSelectedAttributesForModeling cannot be empty");
        }
    }

    public void updateApprovedUsageForAttributes(List<ColumnMetadata> userSelectedAttributesForModeling,
            Table eventTable) {
        eventTable.setName(eventTable.getName() + "_With_UserSelectedAttributesForModeling");
        eventTable.setDisplayName("EventTable_With_UserSelectedAttributesForModeling");

        Map<String, ColumnMetadata> userSelectedAttributesMap = new HashMap<>();
        userSelectedAttributesForModeling.stream() //
                .forEach(attr -> //
        userSelectedAttributesMap.put(attr.getAttrName(), attr));

        for (Attribute eventTableAttribute : eventTable.getAttributes()) {
            if (userSelectedAttributesMap.containsKey(eventTableAttribute.getName())) {
                List<ApprovedUsage> approvedUsage = userSelectedAttributesMap.get(eventTableAttribute.getName())
                        .getApprovedUsageList();
                eventTableAttribute.setApprovedUsage( //
                        CollectionUtils.isEmpty(approvedUsage) //
                                ? new ApprovedUsage[] { ApprovedUsage.NONE } //
                                : approvedUsage.toArray(new ApprovedUsage[approvedUsage.size()]));
            } else {
                log.error(String.format(
                        "Setting ApprovedUsage for Attribute %s as %s because it is not part of user configured attributes for Modeling",
                        eventTableAttribute.getName(), ApprovedUsage.NONE));
                eventTableAttribute.setApprovedUsage( //
                        ApprovedUsage.NONE);
            }
        }

        // TODO - remove following. This was added for extra logging till this feature is stable
        Table accountTable = dataCollectionProxy.getTable(getConfiguration().getCustomerSpace().toString(),
                TableRoleInCollection.ConsolidatedAccount, dataCollectionVersion);
        if (accountTable != null) {
            accountTable.getAttributes().stream().forEach(myAtr -> {
                if (userSelectedAttributesMap.containsKey(myAtr.getName())) {
                    log.info(String.format("%s My Attribute is part of user configured attributes for Modeling",
                            myAtr.getName()));
                } else {
                    log.info(
                            String.format("XXXX %s My Attribute is not part of user configured attributes for Modeling",
                                    myAtr.getName()));
                }
            });
        }
        // remove till here
    }
}
