package com.latticeengines.modeling.workflow.steps;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.AttributeCategoryModifierConfiguration;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("attributeCategoryModifier")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class AttributeCategoryModifier extends BaseWorkflowStep<AttributeCategoryModifierConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(AttributeCategoryModifier.class);

    @Inject
    private MetadataProxy metadataProxy;

    private String customerSpace;

    private boolean excludeDataCloudAttributes;

    private boolean excludeCDLAttributes;

    private static List<Category> fullAttributeDropForCategories = //
            Arrays.asList(Category.DEFAULT);

    @Override
    public void onConfigurationInitialized() {
        customerSpace = configuration.getCustomerSpace().toString();
        excludeDataCloudAttributes = configuration.isExcludeDataCloudAttributes();
        excludeCDLAttributes = configuration.isExcludeCDLAttributes();

        log.info(String.format("excludeDataCloudAttributes = %s, excludeCDLAttributes = %s", excludeDataCloudAttributes,
                excludeCDLAttributes));
    }

    @Override
    public void execute() {
        Table eventTable = getObjectFromContext(EVENT_TABLE, Table.class);
        log.info("Attributes from event table:" + JsonUtils.serialize(eventTable.getAttributes()));
        List<Attribute> attributes = eventTable.getAttributes();
        if (CollectionUtils.isNotEmpty(attributes)) {
            modifyAttributeCategoryIfNeeded(attributes);
            modifyMyAttributeToAccountCategory(attributes);
            markLDCCategoryHiddenIfNeeded(excludeDataCloudAttributes, attributes);
            metadataProxy.updateTable(customerSpace, eventTable.getName(), eventTable);
            putObjectInContext(EVENT_TABLE, eventTable);
        } else {
            throw new RuntimeException("userSelectedAttributesForModeling cannot be empty");
        }
    }

    private void modifyAttributeCategoryIfNeeded(List<Attribute> attributes) {
        attributes.stream().forEach(atr -> {
            Category category = Category.DEFAULT;
            if (atr.getCategory() != null) {
                category = Category.fromName(atr.getCategory());
            }
            if (category.isHiddenFromUi()) {
                if (fullAttributeDropForCategories.contains(category)) {
                    log.info(String.format("Marking attribute '%s' (category '%s') hidden from remodeling UI",
                            atr.getName(), category.name()));
                    atr.setIsHiddenForRemodelingUI(true);
                } else {
                    Category accAttributeCategory = Category.ACCOUNT_ATTRIBUTES;
                    log.info(String.format("Moving attribute %s of Category '%s' under '%s' category", atr.getName(),
                            category.name(), accAttributeCategory.name()));

                    atr.setCategory(accAttributeCategory.name());
                }
            }
        });
    }

    private void modifyMyAttributeToAccountCategory(List<Attribute> attributes) {
        // workaround for PLS-11330 - if attribute category conflicts with
        // display name of ACCOUNT_ATTRIBUTES category then change attribute
        // category to ACCOUNT_ATTRIBUTES
        String myAttributeCategory = Category.ACCOUNT_ATTRIBUTES.getName();

        attributes.stream() //
                .filter(atr -> myAttributeCategory.equals(atr.getCategory())) //
                .forEach(atr -> {
                    Category accAttributeCategory = Category.ACCOUNT_ATTRIBUTES;
                    log.info(String.format("Moving attribute %s of Category '%s' under '%s' category", atr.getName(),
                            myAttributeCategory, accAttributeCategory.name()));

                    atr.setCategory(accAttributeCategory.name());
                });
    }

    private void markLDCCategoryHiddenIfNeeded(boolean excludeDataCloudAttributes, List<Attribute> attributes) {
        if (excludeDataCloudAttributes) {
            attributes.stream().forEach(atr -> {
                Category category = Category.DEFAULT;
                if (atr.getCategory() != null) {
                    category = Category.fromName(atr.getCategory());
                }
                if (category.isLdcReservedCategory()) {
                    log.info(String.format(
                            "Marking attribute '%s' (category '%s') hidden from remodeling UI as data stores has excluded LDC sttributes",
                            atr.getName(), category.name()));
                    atr.setIsHiddenForRemodelingUI(true);
                }
            });
        }
    }
}
