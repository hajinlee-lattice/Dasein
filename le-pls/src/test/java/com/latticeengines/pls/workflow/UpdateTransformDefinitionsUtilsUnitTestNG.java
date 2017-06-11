package com.latticeengines.pls.workflow;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.transform.TransformationPipeline;

public class UpdateTransformDefinitionsUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testUpdateTransformDefinitions() {

        List<TransformDefinition> originalTransformDefinitions = new ArrayList<TransformDefinition>();
        TransformationPipeline.getTransforms(TransformationGroup.STANDARD).stream().forEach(
                a -> originalTransformDefinitions.add(new TransformDefinition(a.name, a.output, a.type, a.arguments)));

        List<TransformDefinition> udpatedTransformDefinitions = UpdateTransformDefinitionsUtils
                .updateTransformDefinitions(TransformationGroup.STANDARD, TransformationPipeline.PACKAGE_NAME);
        List<TransformDefinition> differentDefs = findDifferentTransformDefinition(originalTransformDefinitions,
                udpatedTransformDefinitions);
        Assert.assertEquals(differentDefs.size(), 11);
        Assert.assertEquals(differentDefs.stream()
                .filter(def -> def.transformationMetadata.getCategory().equals(Category.ACCOUNT_INFORMATION.toString()))
                .count(), 11);
    }

    private List<TransformDefinition> findDifferentTransformDefinition(
            List<TransformDefinition> originalTransformDefinitions,
            List<TransformDefinition> updatedTransformDefinitions) {
        return updatedTransformDefinitions.stream()
                .filter(def -> !originalDefIsTheSameWithUpdated(def, originalTransformDefinitions))
                .collect(Collectors.toList());
    }

    private boolean originalDefIsTheSameWithUpdated(TransformDefinition originalDef,
            List<TransformDefinition> updatedTransformDefinitions) {
        boolean foundMatch = false;
        foundMatch = updatedTransformDefinitions.stream().anyMatch(a -> equalDefinitions(a, originalDef));
        return foundMatch;
    }

    private boolean equalDefinitions(TransformDefinition d1, TransformDefinition d2) {
        return d1.equals(d2) && //
                ((d1.transformationMetadata == null && d2.transformationMetadata == null)
                        || (d1.transformationMetadata != null && d2.transformationMetadata != null
                                && d1.transformationMetadata.equals(d2.transformationMetadata)));
    }

}
