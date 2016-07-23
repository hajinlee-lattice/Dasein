package com.latticeengines.metadata.validation.service.impl;

import java.io.IOException;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;

public class PivotMappingFileValidationServiceImplTestNG extends MetadataFunctionalTestNGBase {

    private static final String RESOURCE_BASE = "com/latticeengines/artifact/validation";

    private String hdfsPath = "/tmp/artifact";

    @Test(groups = "functional")
    public void test() throws IOException {
        HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, ClassLoader.getSystemResource(RESOURCE_BASE + "/pivot.csv")
                .getPath(), hdfsPath);
        String error = ArtifactValidation.getArtifactValidationService(ArtifactType.PivotMapping).validate(
                hdfsPath + "/pivot.csv");
        assertEquals(error,
                "Unable to find required columns [SourceColumn, TargetColumn, SourceColumnType] from the file");

        HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration,
                ClassLoader.getSystemResource(RESOURCE_BASE + "/pivotvalues.csv").getPath(), hdfsPath);
        error = ArtifactValidation.getArtifactValidationService(ArtifactType.PivotMapping).validate(
                hdfsPath + "/pivotvalues.csv");

        assertEquals(error, "");
    }
}
