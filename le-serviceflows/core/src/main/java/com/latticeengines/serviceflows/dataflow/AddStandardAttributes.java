package com.latticeengines.serviceflows.dataflow;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.core.dataflow.AddStandardAttributesParameters;
import com.latticeengines.domain.exposed.transform.TransformationMetadata;
import com.latticeengines.domain.exposed.transform.TransformationPipeline;

@Component("addStandardAttributes")
public class AddStandardAttributes extends TypesafeDataFlowBuilder<AddStandardAttributesParameters> {

    private static final Logger log = LoggerFactory.getLogger(AddStandardAttributes.class);

    @Override
    public Node construct(AddStandardAttributesParameters parameters) {
        Node eventTable = addSource(parameters.eventTable);
        Node last = eventTable;

        List<TransformDefinition> definitions = parameters.transforms;

        fixTransformArgumentsAndMetadata(eventTable, definitions, parameters.sourceSchemaInterpretation);

        for (TransformDefinition definition : definitions) {
            resolveDuplicateName(eventTable, definition);
            last = addFunction(last, eventTable, definition, parameters.inputSkippedAttributeList);
        }

        if (parameters.doSort) {
            log.info("Sorting event table.");
            last = last.sort("InternalId", true);
        }

        return last;
    }

    private void fixTransformArgumentsAndMetadata(Node eventTable, List<TransformDefinition> definitions,
            SchemaInterpretation schema) {
        fixStdLengthDomainArgs(eventTable, definitions.stream()
                .filter(a -> a.output.equals(TransformationPipeline.stdLengthDomain.output)).findFirst().orElse(null),
                schema);
        fixStdVisidbDsIndustryGroupArgs(eventTable,
                definitions.stream()
                        .filter(a -> a.output.equals(TransformationPipeline.stdVisidbDsIndustryGroup.output))
                        .findFirst().orElse(null));
    }

    private void fixStdLengthDomainArgs(Node eventTable, TransformDefinition domainLength,
            SchemaInterpretation schema) {
        log.info("Fixing Domain Length");
        if (domainLength == null) {
            log.info("Domain Length is null");
            return;
        }

        Attribute websiteOrEmail = eventTable.getSourceAttribute(InterfaceName.Website);

        if (websiteOrEmail == null) {
            websiteOrEmail = eventTable.getSourceAttribute(InterfaceName.Email);
        }
        log.info("websiteOrEmail is: " + (websiteOrEmail != null ? websiteOrEmail.getName() : "null"));
        if (websiteOrEmail != null && !domainLength.arguments.isEmpty()) {
            domainLength.arguments.put("column", websiteOrEmail.getName());
            log.info("set domain_length arguments to: " + domainLength.arguments);
        } else if (!domainLength.arguments.isEmpty()) {
            domainLength.arguments.put("column", "");
        }
    }

    private void fixStdVisidbDsIndustryGroupArgs(Node eventTable, TransformDefinition stdVisidbDsIndustryGroup) {
        Attribute industryOrDataCloudIndustry = eventTable.getSourceAttribute(InterfaceName.Industry);

        if (industryOrDataCloudIndustry == null) {
            industryOrDataCloudIndustry = eventTable.getSourceAttribute("ConsolidatedIndustry");
        } else {
            return;
        }

        if (stdVisidbDsIndustryGroup == null || industryOrDataCloudIndustry == null) {
            return;
        }

        stdVisidbDsIndustryGroup.arguments.put("column", industryOrDataCloudIndustry.getName());
        TransformationMetadata metadata = new TransformationMetadata();
        metadata.setTags(Tag.EXTERNAL_TRANSFORM);
        metadata.setCategory(Category.FIRMOGRAPHICS);
        stdVisidbDsIndustryGroup.transformationMetadata = metadata;
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

    private Node addFunction(Node last, Node eventTable, TransformDefinition definition,
            List<String> inputSkippedAttributeList) {
        log.info("definition: " + definition);
        for (Object value : definition.arguments.values()) {
            Attribute attr = eventTable.getSourceAttribute(String.valueOf(value));
            if (attr == null) {
                log.info(String.format("Excluding field %s (function %s) because some source columns are not available",
                        definition.output, definition.name));
                return last;
            }
            if (CollectionUtils.isNotEmpty(inputSkippedAttributeList)
                    && inputSkippedAttributeList.contains(attr.getName())) {
                log.info(String.format("Excluding field %s (function %s) because some source columns are skipped",
                        definition.output, definition.name));
                return last;
            }
        }
        return last.addTransformFunction(TransformationPipeline.PACKAGE_NAME, definition);
    }
}
