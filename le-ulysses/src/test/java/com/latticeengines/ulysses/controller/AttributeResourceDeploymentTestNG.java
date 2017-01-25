package com.latticeengines.ulysses.controller;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.attribute.PrimaryField;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretationCollections;
import com.latticeengines.ulysses.testframework.UlyssesDeploymentTestNGBase;

public class AttributeResourceDeploymentTestNG extends UlyssesDeploymentTestNGBase {

    private static final Logger log = Logger.getLogger(AttributeResourceDeploymentTestNG.class);

    private static final ObjectMapper OM = new ObjectMapper();

    private String getAttributeResourceUrl() {
        return ulyssesHostPort + "/ulysses/attributes";
    }

    @Test(groups = "deployment")
    public void testGetPrimaryAttributes() {
        String url = getAttributeResourceUrl() + "/primary";
        List<?> genericList = getOAuth2RestTemplate().getForObject(url, List.class);
        List<PrimaryField> primaryFields = OM.convertValue(genericList, new TypeReference<List<PrimaryField>>() {
        });
        log.info("Converted Primary Fields: " + primaryFields);
        Assert.assertNotNull(primaryFields);
        Assert.assertTrue(primaryFields.size() > 0);
        Assert.assertEquals(FieldInterpretationCollections.PrimaryMatchingFields.size(), primaryFields.size());
        // Create a temporary set with Primary Field Names
        Set<String> fieldNames = new HashSet<>();
        for (PrimaryField field : primaryFields) {
            fieldNames.add(field.getFieldName());
        }
        // Make sure that all the values from EnumSet are returned in response
        for (FieldInterpretation field : FieldInterpretationCollections.PrimaryMatchingFields) {
            Assert.assertTrue(fieldNames.contains(field.getFieldName()));
        }
    }

    @Test(groups = "deployment")
    public void testGetPrimaryAttributesValidationExpressionSimplified() {
        String url = getAttributeResourceUrl() + "/primary/validation-expression";
        String expression = getOAuth2RestTemplate().getForObject(url, String.class);
        log.info("Primary Fields Expression: " + expression);
        Assert.assertNotNull(expression);
        Assert.assertEquals(expression, "((Website||Email||CompanyName)&&(Id))");
    }
}
