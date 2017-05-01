package com.latticeengines.leadprioritization.workflow;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.dataflow.exposed.builder.operations.GetAndValidateRealTimeTransformUtils;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.transform.TransformationMetadata;
import com.latticeengines.domain.exposed.transform.TransformationPipeline;
import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;

public class UpdateTransformDefinitionsUtils {

    private static final Log log = LogFactory.getLog(UpdateTransformDefinitionsUtils.class);

    public static List<TransformDefinition> updateTransformDefinitions(TransformationGroup transformationGroup) {
        List<TransformDefinition> transformDefinitions = TransformationPipeline.getTransforms(transformationGroup);
        transformDefinitions.stream()
                .forEach(UpdateTransformDefinitionsUtils::overWriteTransformDefinitionAccountCategory);
        transformDefinitions.stream().forEach(UpdateTransformDefinitionsUtils::printTransformDeifitionAndCategoryInfo);
        return transformDefinitions;
    }

    private static void printTransformDeifitionAndCategoryInfo(TransformDefinition transformDefinition) {
        log.info(String.format("%s: %s", transformDefinition.name, transformDefinition.transformationMetadata == null
                ? "null" : transformDefinition.transformationMetadata.getCategory()));
    }

    private static void overWriteTransformDefinitionAccountCategory(TransformDefinition transformDefinition) {
        RealTimeTransform transform = GetAndValidateRealTimeTransformUtils
                .fetchAndValidateRealTimeTransform(transformDefinition);
        TransformMetadata metadata = transform.getMetadata();
        if (metadata != null) {
            Map<String, String> properties = metadata.getProperties();
            if (properties != null //
                    && properties.containsKey("Category") //
                    && properties.get("Category").equals(Category.LEAD_INFORMATION.getName())//
            ) {
                TransformationMetadata defintionMetadata = new TransformationMetadata();
                defintionMetadata.setCategory(Category.ACCOUNT_INFORMATION);
                transformDefinition.transformationMetadata = defintionMetadata;
                log.info(String.format("For definition %s, the category is %s", transformDefinition.name,
                        defintionMetadata.getCategory()));
            }
        }
    }
}
