package com.latticeengines.metadata.service.impl;

import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.expection.AnnotationValidationError;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.metadata.service.MetadataService;

public class MetadataServiceImplUnitTestNG {
    
    private MetadataService metadataService = new MetadataServiceImpl();
    
    @BeforeClass(groups = "unit")
    public void setup() {
    }

    @Test(groups = "unit")
    public void validateTableMetadata() throws Exception {
        String metadataFile = ClassLoader.getSystemResource(
                "com/latticeengines/metadata/controller/invalidmetadata.avsc").getPath();
        ModelingMetadata metadata = JsonUtils.deserialize(FileUtils.readFileToString(new File(metadataFile)), ModelingMetadata.class);
        Map<String, Set<AnnotationValidationError>> errors = metadataService.validateTableMetadata(null, metadata);
        assertTrue(errors.size() > 0);
    }
}
