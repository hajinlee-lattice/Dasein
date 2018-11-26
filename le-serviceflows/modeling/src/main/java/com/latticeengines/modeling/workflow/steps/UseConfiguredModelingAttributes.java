package com.latticeengines.modeling.workflow.steps;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.UseConfiguredModelingAttributesConfiguration;
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

    private DataCollection.Version dataCollectionVersion;

    private String customerSpace;

    private boolean excludeDataCloudAttributes;

    private boolean excludeCDLAttributes;

    private RatingEngineType ratingEngineType;

    @Value("${cdl.modeling.product.spent.special.handling:true}")
    private Boolean doSpecialHandlingForProductSpent;

    private Set<RatingEngineType> typesUsingProductSpent = //
            Sets.newHashSet(RatingEngineType.CROSS_SELL);

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
        ratingEngineType = configuration.getRatingEngineType();

        log.info(String.format(
                "excludeDataCloudAttributes = %s, excludeCDLAttributes = %s, doSpecialHandlingForProductSpent=%s",
                excludeDataCloudAttributes, excludeCDLAttributes, doSpecialHandlingForProductSpent));
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
            Set<String> attributesWithModelAndAllInsights = updateApprovedUsageForAttributes(
                    userSelectedAttributesForModeling, eventTable);
            updateApprovedUsageForDependentCuratedAttributes(eventTable, attributesWithModelAndAllInsights);

            metadataProxy.updateTable(customerSpace, eventTable.getName(), eventTable);
            putObjectInContext(EVENT_TABLE, eventTable);
        } else {
            throw new RuntimeException("userSelectedAttributesForModeling cannot be empty");
        }
    }

    public Set<String> updateApprovedUsageForAttributes(List<ColumnMetadata> userSelectedAttributesForModeling,
            Table eventTable) {
        eventTable.setName(eventTable.getName() + "_sel_attrs");
        eventTable.setDisplayName("EventTable_With_UserSelectedAttributesForModeling");

        Map<String, ColumnMetadata> userSelectedAttributesMap = new HashMap<>();
        userSelectedAttributesForModeling.forEach(attr -> userSelectedAttributesMap.put(attr.getAttrName(), attr));
        Set<String> attributesWithModelAndAllInsights = new HashSet<>();

        for (Attribute eventTableAttribute : eventTable.getAttributes()) {
            if (Category.PRODUCT_SPEND.getName().equals(eventTableAttribute.getCategory())
                    && doSpecialHandlingForProductSpent) {
                if (typesUsingProductSpent.contains(ratingEngineType)) {
                    List<ApprovedUsage> approvedUsage = Collections.singletonList(ApprovedUsage.MODEL_ALLINSIGHTS);
                    if (userSelectedAttributesMap.containsKey(eventTableAttribute.getName())) {
                        approvedUsage = userSelectedAttributesMap.get(eventTableAttribute.getName())
                                .getApprovedUsageList();
                    }

                    log.info(String.format("Overwrite ApprovedUsage for Attribute %s (Category '%s') as %s",
                            eventTableAttribute.getName(), eventTableAttribute.getCategory(),
                            JsonUtils.serialize(approvedUsage)));
                    attributesWithModelAndAllInsights.add(eventTableAttribute.getName());
                    eventTableAttribute
                            .setApprovedUsage(approvedUsage.toArray(new ApprovedUsage[0]));
                } else {
                    log.info(String.format(
                            "Setting ApprovedUsage for Attribute %s (Category '%s') as %s because %s attributes should not be used "
                                    + "for modeling of Rating Engine type '%s'",
                            eventTableAttribute.getName(), eventTableAttribute.getCategory(), ApprovedUsage.NONE,
                            eventTableAttribute.getCategory(), ratingEngineType));
                    eventTableAttribute.setApprovedUsage(ApprovedUsage.NONE);
                }
            } else {
                if (userSelectedAttributesMap.containsKey(eventTableAttribute.getName()) //
                        && CollectionUtils.isNotEmpty( //
                                userSelectedAttributesMap.get(eventTableAttribute.getName()).getApprovedUsageList())) {
                    List<ApprovedUsage> approvedUsage = userSelectedAttributesMap.get(eventTableAttribute.getName())
                            .getApprovedUsageList();
                    attributesWithModelAndAllInsights.add(eventTableAttribute.getName());
                    eventTableAttribute
                            .setApprovedUsage(approvedUsage.toArray(new ApprovedUsage[0]));
                } else {
                    log.info(String.format(
                            "Setting ApprovedUsage for Attribute %s (Category '%s') as %s because it is not part of user "
                                    + "configured attributes for Modeling",
                            eventTableAttribute.getName(), eventTableAttribute.getCategory(), ApprovedUsage.NONE));
                    eventTableAttribute.setApprovedUsage(ApprovedUsage.NONE);
                }
            }
        }
        return attributesWithModelAndAllInsights;
    }

    private void updateApprovedUsageForDependentCuratedAttributes(Table eventTable,
            Set<String> attributesWithModelAndAllInsights) {
        for (Attribute eventTableAttribute : eventTable.getAttributes()) {
            if (CollectionUtils.isNotEmpty(eventTableAttribute.getParentAttributeNames())) {
                if (attributesWithModelAndAllInsights.containsAll(eventTableAttribute.getParentAttributeNames())) {
                    log.info(String.format(
                            "Setting ApprovedUsage for curated Attribute %s (Category '%s') as %s because all of its parent attributes have approved usage as %s",
                            eventTableAttribute.getName(), eventTableAttribute.getCategory(),
                            ApprovedUsage.MODEL_ALLINSIGHTS, ApprovedUsage.MODEL_ALLINSIGHTS));
                    eventTableAttribute.setApprovedUsage( //
                            ApprovedUsage.MODEL_ALLINSIGHTS);
                } else {
                    log.info(String.format(
                            "Setting ApprovedUsage for curated Attribute %s (Category '%s') as %s because not all of its parent attributes have approved usage as %s",
                            eventTableAttribute.getName(), eventTableAttribute.getCategory(), ApprovedUsage.NONE,
                            ApprovedUsage.MODEL_ALLINSIGHTS));
                    eventTableAttribute.setApprovedUsage( //
                            ApprovedUsage.NONE);
                }
            }
        }
    }
}
