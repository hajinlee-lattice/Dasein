package com.latticeengines.metadata.validation.service.impl;

import static org.testng.Assert.assertTrue;

import java.io.IOException;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;

public class PmmlFileValidationServiceImpTestNG extends MetadataFunctionalTestNGBase {

    private static final String RESOURCE_BASE = "com/latticeengines/artifact/validation";

    private String hdfsPath = "/tmp/artifact";

    @Test(groups = "functional")
    public void testPmml() throws IOException {
        HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration,
                ClassLoader.getSystemResource(RESOURCE_BASE + "/IRIS_MLP_Neural_Network.xml").getPath(), hdfsPath);

        String error = ArtifactValidation.getArtifactValidationService(ArtifactType.PMML).validate(
                hdfsPath + "/IRIS_MLP_Neural_Network.xml");
        System.out.println(error);
        assertTrue(error.contains("MiningField:petal length has invalid value."));
        assertTrue(error.contains("DataField:petal width has invalid value."));
        HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
    }

}
