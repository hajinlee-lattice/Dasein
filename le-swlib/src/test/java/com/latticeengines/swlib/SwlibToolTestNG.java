package com.latticeengines.swlib;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.swlib.SoftwarePackage;

public class SwlibToolTestNG {
    
    private static final String INITIALIZER_CLASS = "com.latticeengines.serviceflows.prospectdiscovery.Initializer";

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        Configuration config = new Configuration();
        HdfsUtils.rmdir(config, "/app/swlib/dataflow");
    }

    @Test(groups = "functional")
    public void main() throws Exception {
        String jarFile = ClassLoader
                .getSystemResource("com/latticeengines/swlib/le-serviceflows-prospectdiscovery.jar").getPath();
        SwlibTool.main(new String[] { "-o", "install", //
                "-m", "dataflow", //
                "-g", "com.latticeengines", //
                "-a", "le-serviceflows", //
                "-v", "1.0.0", //
                "-i", INITIALIZER_CLASS, //
                "-h", "hdfs://localhost:9000", //
                "-f", jarFile });
        assertTrue(HdfsUtils.fileExists(new Configuration(), //
                "/app/swlib/dataflow/com/latticeengines/le-serviceflows/1.0.0/le-serviceflows-1.0.0.jar"));
        assertTrue(HdfsUtils.fileExists(new Configuration(), //
                "/app/swlib/dataflow/com/latticeengines/le-serviceflows/1.0.0/le-serviceflows-1.0.0.json"));
        String softwarePackageStr = HdfsUtils.getHdfsFileContents(new Configuration(), //
                "/app/swlib/dataflow/com/latticeengines/le-serviceflows/1.0.0/le-serviceflows-1.0.0.json");
        SoftwarePackage softwarePackage = JsonUtils.deserialize(softwarePackageStr, SoftwarePackage.class);
        assertEquals(softwarePackage.getInitializerClass(), INITIALIZER_CLASS);
    }
}
