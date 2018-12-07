package com.latticeengines.ulysses.controller;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.attribute.PrimaryField;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretationCollections;
import com.latticeengines.domain.exposed.ulysses.PrimaryFieldConfiguration;
import com.latticeengines.ulysses.testframework.UlyssesDeploymentTestNGBase;

public class AttributeResourceDeploymentTestNG extends UlyssesDeploymentTestNGBase {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private String getAttributeResourceUrl() {
        return ulyssesHostPort + "/ulysses/attributes";
    }

    @SuppressWarnings("deprecation")
    @Test(groups = "deployment")
    public void testGetPrimaryAttributeConfiguration() {
        String url = getAttributeResourceUrl() + "/primaryfield-configuration";
        PrimaryFieldConfiguration primaryFieldConfig = getOAuth2RestTemplate().getForObject(url,
                PrimaryFieldConfiguration.class);
        List<PrimaryField> primaryFields = primaryFieldConfig.getPrimaryFields();
        log.info("Primary Fields: " + primaryFields);
        verifyPrimaryFields(primaryFields);
        Assert.assertNotNull(primaryFieldConfig.getValidationExpression());
        log.info("Primary Fields Validation Expression: "
                + primaryFieldConfig.getValidationExpression().getExpression());
        FeatureFlagValueMap ffMap = getFeatureFlags();
        if (ffMap.get(LatticeFeatureFlag.ENABLE_FUZZY_MATCH.getName())) {
            Assert.assertEquals(primaryFieldConfig.getValidationExpression().getExpression(),
                    FieldInterpretationCollections.FUZZY_MATCH_VALIDATION_EXPRESSION);
        } else {
            Assert.assertEquals(primaryFieldConfig.getValidationExpression().getExpression(),
                    FieldInterpretationCollections.NON_FUZZY_MATCH_VALIDATION_EXPRESSION);
        }
    }

    private void verifyPrimaryFields(List<PrimaryField> primaryFields) {
        Assert.assertNotNull(primaryFields);
        Assert.assertTrue(primaryFields.size() > 0);
        Assert.assertEquals(FieldInterpretationCollections.PrimaryMatchingFields.size(),
                primaryFields.size());
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

    private FeatureFlagValueMap getFeatureFlags() {
        FeatureFlagValueMap map = getOAuth2RestTemplate().getForObject(
                getUlyssesRestAPIPort() + "/ulysses/tenant/featureflags",
                FeatureFlagValueMap.class);
        return map;
    }
}
