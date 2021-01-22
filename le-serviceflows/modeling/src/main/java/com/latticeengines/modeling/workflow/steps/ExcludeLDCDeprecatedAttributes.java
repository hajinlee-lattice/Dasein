package com.latticeengines.modeling.workflow.steps;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.ExcludeLDCDeprecatedAttributesConfiguration;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("excludeLDCDeprecatedAttributes")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExcludeLDCDeprecatedAttributes extends BaseWorkflowStep<ExcludeLDCDeprecatedAttributesConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(ExcludeLDCDeprecatedAttributes.class);

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private BatonService batonService;

    @Override
    public void execute() {
        log.info("Starting ExcludeLDCDeprecatedAttributes");

        Set<String> attributesDeprecated = requestDeprecatedAttributes();

        ExcludeLDCDeprecatedAttributesConfiguration configuration = getConfiguration();
        Table eventTable = getObjectFromContext(EVENT_TABLE, Table.class);

        for (Attribute attribute : eventTable.getAttributes()) {
            if (attributesDeprecated.contains(attribute.getName())) {
                attribute.setApprovedUsage(ApprovedUsage.NONE);
            }
        }

        putObjectInContext(EVENT_TABLE, eventTable);
        metadataProxy.updateTable(configuration.getCustomerSpace().toString(), eventTable.getName(), eventTable);
    }

    private Set<String> requestDeprecatedAttributes() {
        Set<String> expiredLicenses = batonService.getExpiredLicenses(configuration.getCustomerSpace().getTenantId());
        Set<String> attributesDeprecated = new HashSet<>();
        List<ColumnMetadata> metadata = columnMetadataProxy.columnSelection(ColumnSelection.Predefined.Model);
        if (CollectionUtils.isNotEmpty(metadata)) {
            for (ColumnMetadata column : metadata) {
                if (column != null && (Boolean.TRUE
                        .equals(column.getShouldDeprecate()) || expiredLicenses.contains(column.getDataLicense()))) {
                    attributesDeprecated.add(column.getAttrName());
                }
            }
        } else {
            log.warn("ExcludeLDCDeprecatedAttributes: ColumnMetadataProxy.columnSelection returned an empty column set.");
        }

        return attributesDeprecated;
    }
}
