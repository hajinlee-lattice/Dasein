package com.latticeengines.leadprioritization.dataflow;

import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.dataflow.flows.AddStandardAttributesParameters;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.transform.TransformationMetadata;
import com.latticeengines.domain.exposed.transform.TransformationPipeline;
import com.latticeengines.serviceflows.dataflow.util.DataFlowUtils;

@Component("addStandardAttributes")
public class AddStandardAttributes extends TypesafeDataFlowBuilder<AddStandardAttributesParameters> {

    private static final String DOMAIN = "__Domain";
    private static final Log log = LogFactory.getLog(AddStandardAttributes.class);

    @Override
    public Node construct(AddStandardAttributesParameters parameters) {
        Node eventTable = addSource(parameters.eventTable);

        fixTransformArgumentsAndMetadata(eventTable);

        Node last = fixStdLengthDomainArgs(eventTable);

        Set<TransformDefinition> definitions = TransformationPipeline.getTransforms(parameters.transformationGroup);

        for (TransformDefinition definition : definitions) {
            resolveDuplicateName(eventTable, definition);
            last = addFunction(last, definition);
        }

        if (parameters.doSort) {
            log.info("Sorting event table.");
            setEnforceGlobalOrdering(true);
            last = last.sort("InternalId", true);
        }

        return last.discard(new FieldList(DOMAIN));
    }

    private void fixTransformArgumentsAndMetadata(Node eventTable) {
        fixStdVisidbDsIndustryGroupArgs(eventTable);
    }

    private Node fixStdLengthDomainArgs(Node eventTable) {
        Node newEventTableWithDomain = eventTable;
        List<String> fieldNames = eventTable.getFieldNames();
        if (fieldNames.contains(InterfaceName.Domain.toString())
                || fieldNames.contains(InterfaceName.Website.toString())
                || fieldNames.contains(InterfaceName.Email.toString())) {
            newEventTableWithDomain = DataFlowUtils.extractDomain(eventTable, DOMAIN);
            TransformationPipeline.stdLengthDomain.arguments.put("column", DOMAIN);
        } else {
            TransformationPipeline.stdLengthDomain.arguments.put("column", "");
        }

        return newEventTableWithDomain;
    }

    private void fixStdVisidbDsIndustryGroupArgs(Node eventTable) {
        Attribute industryOrDataCloudIndustry = eventTable.getSourceAttribute(InterfaceName.Industry);

        if (industryOrDataCloudIndustry == null) {
            industryOrDataCloudIndustry = eventTable.getSourceAttribute("ConsolidatedIndustry");
        } else {
            return;
        }

        if (industryOrDataCloudIndustry == null) {
            return;
        }

        TransformationPipeline.stdVisidbDsIndustryGroup.arguments.put("column", industryOrDataCloudIndustry.getName());
        TransformationMetadata metadata = new TransformationMetadata();
        metadata.setTags(Tag.EXTERNAL_TRANSFORM);
        metadata.setCategory(Category.FIRMOGRAPHICS);
        TransformationPipeline.stdVisidbDsIndustryGroup.transformationMetadata = metadata;
    }

    @VisibleForTesting
    void resolveDuplicateName(Node eventTable, TransformDefinition definition) {
        int version = 1;
        while (eventTable.getSourceAttribute(definition.output) != null) {
            definition.output = String.format("%s_%d", definition.output, version++);
        }
        version = 1;
        while (eventTable.getSourceSchema().getAttributeFromDisplayName(definition.outputDisplayName) != null) {
            definition.outputDisplayName = String.format("%s %d", definition.outputDisplayName, version++);
        }
    }

    private Node addFunction(Node last, TransformDefinition definition) {
        for (Object value : definition.arguments.values()) {
            if (!last.getFieldNames().contains(String.valueOf(value))) {
                log.info(String.format("Excluding field %s (function %s) because some source columns are not available",
                        definition.output, definition.name));
                return last;
            }
        }
        return last.addTransformFunction("com.latticeengines.transform.v2_0_25.functions", definition);
    }
}
