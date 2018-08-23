package com.latticeengines.domain.exposed.swlib;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class SoftwarePackageUnitTestNG {

    @Test(groups = "unit")
    public void getHdfsPathWithoutClassifier() {
        SoftwarePackage pkg = new SoftwarePackage();
        pkg.setModule("dataflow");
        pkg.setGroupId("org.latticeengines");
        pkg.setArtifactId("le-serviceflows");
        
        assertEquals(pkg.getHdfsPath(), "dataflow/le-serviceflows.jar");
    }

    @Test(groups = "unit")
    public void getHdfsPathWithClassifier() {
        SoftwarePackage pkg = new SoftwarePackage();
        pkg.setModule("dataflow");
        pkg.setGroupId("org.latticeengines");
        pkg.setArtifactId("le-serviceflows");
        pkg.setClassifier("shaded");
        
        assertEquals(pkg.getHdfsPath(), "dataflow/le-serviceflows-shaded.jar");
    }
}
