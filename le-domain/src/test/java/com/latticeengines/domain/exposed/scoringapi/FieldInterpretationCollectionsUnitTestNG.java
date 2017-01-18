package com.latticeengines.domain.exposed.scoringapi;

import java.util.EnumSet;

import org.testng.Assert;
import org.testng.annotations.Test;

public class FieldInterpretationCollectionsUnitTestNG {

    @Test(groups = "unit")
    public void testPrimaryMatchingFields() {
        EnumSet<FieldInterpretation> primaryFields = null;
        try {
            primaryFields = FieldInterpretationCollections.PrimaryMatchingFields;
        } catch (Throwable e) {
            Assert.fail("Could not initialize Primary Matching Fields", e);
        }
        Assert.assertNotNull(primaryFields);
        Assert.assertTrue(primaryFields.size() > 0, "PrimaryMatchingFields are not initialized");
    }
    
}
