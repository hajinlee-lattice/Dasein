package com.latticeengines.common.exposed.modeling;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.jython.JythonEngine;

public class ModelExtractorUnitTestNG {
    private static final String ST_PIPELINE_BINARY_P = "/STPipelineBinary.p";
    private static final String TARGETDIR = "/tmp/modelfiles";
    private static final String path = ClassLoader
            .getSystemResource("com/latticeengines/common/exposed/modeling/model.json").getPath();

    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        FileUtils.deleteDirectory(new File(TARGETDIR));
        new File(TARGETDIR).mkdir();
    }

    @Test(groups = "unit")
    public void extractModelArtifacts() throws Exception {
        ModelExtractor extractor = new ModelExtractor();

        extractor.extractModelArtifacts(path, TARGETDIR);

        new JythonEngine(TARGETDIR);
    }

    @Test(groups = "unit")
    public void extractModelArtifactsWithFilter() throws Exception {
        ModelExtractor extractor = new ModelExtractor();

        extractor.extractModelArtifacts(path, TARGETDIR);
        Assert.assertTrue(new File(TARGETDIR + ST_PIPELINE_BINARY_P).exists());
        this.setup();

        extractor.extractModelArtifacts(path, TARGETDIR, (dir, name) -> !name.equals("STPipelineBinary.p"));
        Assert.assertFalse(new File(TARGETDIR + ST_PIPELINE_BINARY_P).exists());
    }

}
