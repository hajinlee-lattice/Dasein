package com.latticeengines.common.exposed.modeling;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.jython.JythonEngine;

public class ModelExtractorUnitTestNG {
    private static final String TARGETDIR = "/tmp/modelfiles";
    
    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        FileUtils.deleteDirectory(new File(TARGETDIR));
        new File(TARGETDIR).mkdir();
    }

    @Test(groups = "unit")
    public void extractModelArtifacts() throws Exception {
        String path = ClassLoader.getSystemResource("com/latticeengines/common/exposed/modeling/model.json").getPath();
        ModelExtractor extractor = new ModelExtractor();
        
        extractor.extractModelArtifacts(path, TARGETDIR);
        
        new JythonEngine(TARGETDIR);
    }
}
