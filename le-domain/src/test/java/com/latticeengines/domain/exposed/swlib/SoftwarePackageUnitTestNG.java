package com.latticeengines.domain.exposed.swlib;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class SoftwarePackageUnitTestNG {

    @Test(groups = "unit")
    public void getHdfsJarPathWithoutClassifier() {
        SoftwarePackage pkg = new SoftwarePackage();
        pkg.setModule("dataflow");
        pkg.setGroupId("org.latticeengines");
        pkg.setArtifactId("le-serviceflows");
        pkg.setVersion("1.0.0");
        
        assertEquals(pkg.getHdfsPath(), "dataflow/org/latticeengines/le-serviceflows/1.0.0/le-serviceflows-1.0.0.jar");
    }

    @Test(groups = "unit")
    public void getHdfsJarPathWithClassifier() {
        SoftwarePackage pkg = new SoftwarePackage();
        pkg.setModule("dataflow");
        pkg.setGroupId("org.latticeengines");
        pkg.setArtifactId("le-serviceflows");
        pkg.setVersion("1.0.0");
        pkg.setClassifier("shaded");
        
        assertEquals(pkg.getHdfsPath(), "dataflow/org/latticeengines/le-serviceflows/1.0.0/le-serviceflows-1.0.0-shaded.jar");
    }
}
