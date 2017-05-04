package com.latticeengines.leadprioritization.wokflow;

import java.util.LinkedHashMap;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.transform.TransformationPipeline;
import com.latticeengines.domain.exposed.util.GetAndValidateRealTimeTransformUtils;
import com.latticeengines.transform.exposed.RealTimeTransform;

public class GetAndValidateRealTimeTransformUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testFetchAndValidateRealTimeTransform() {
        TransformDefinition stdVisidbDsCompanynameEntropy = new TransformDefinition("StdVisidbDsCompanynameEntropy",
                "CompanyName_Entropy", FieldType.FLOAT, new LinkedHashMap<String, Object>());
        RealTimeTransform transform = GetAndValidateRealTimeTransformUtils
                .fetchAndValidateRealTimeTransform(stdVisidbDsCompanynameEntropy, TransformationPipeline.PACKAGE_NAME);
        Assert.assertNotNull(transform);
    }

    @Test(groups = "unit", expectedExceptions = RuntimeException.class)
    public void testUnhappyPath() {
        TransformDefinition someRandomDef = new TransformDefinition("someRandomDef", "someRandomDef", FieldType.INTEGER,
                new LinkedHashMap<String, Object>());
        GetAndValidateRealTimeTransformUtils.fetchAndValidateRealTimeTransform(someRandomDef,
                TransformationPipeline.PACKAGE_NAME);
        Assert.fail("Should have thrown exception");
    }
}
