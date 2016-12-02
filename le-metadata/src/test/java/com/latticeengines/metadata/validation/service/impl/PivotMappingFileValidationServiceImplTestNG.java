package com.latticeengines.metadata.validation.service.impl;

import java.io.IOException;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;

public class PivotMappingFileValidationServiceImplTestNG extends MetadataFunctionalTestNGBase {

    private static final String RESOURCE_BASE = "com/latticeengines/artifact/validation";

    private String hdfsPath = "/tmp/artifact";

    @Test(groups = "functional")
    public void testPivotMappingFile() throws IOException {
        HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration,
                ClassLoader.getSystemResource(RESOURCE_BASE + "/pivot.csv").getPath(), hdfsPath);
        try {
            ArtifactValidation.getArtifactValidationService(ArtifactType.PivotMapping)
                    .validate(hdfsPath + "/pivot.csv");
            assertTrue(false);
        } catch (Exception e) {
            assertEquals(e.getCause().getMessage(),
                    "Unable to find required columns [SourceColumn, TargetColumn, SourceColumnType] from the file");
        }

        HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration,
                ClassLoader.getSystemResource(RESOURCE_BASE + "/pivotvalues.csv").getPath(), hdfsPath);

        ArtifactValidation.getArtifactValidationService(ArtifactType.PivotMapping)
                .validate(hdfsPath + "/pivotvalues.csv");

        HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration,
                ClassLoader.getSystemResource(RESOURCE_BASE + "/PMML-large-file.csv").getPath(), hdfsPath);

        try {
            ArtifactValidation.getArtifactValidationService(ArtifactType.PivotMapping)
                    .validate(hdfsPath + "/PMML-large-file.csv");
            assertTrue(false);
        } catch (Exception e) {
            assertEquals(e.getCause().getMessage(), "Found unsupported character in \" LeadSource\" in Pivot Mapping File.");
        }
    }
}
