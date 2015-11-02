package com.latticeengines.swlib;

import static org.testng.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;

public class SwlibToolTestNG {

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
                "-f", jarFile });
        assertTrue(HdfsUtils.fileExists(new Configuration(), //
                "/app/swlib/dataflow/com/latticeengines/le-serviceflows/1.0.0/le-serviceflows-1.0.0.jar"));
        assertTrue(HdfsUtils.fileExists(new Configuration(), //
                "/app/swlib/dataflow/com/latticeengines/le-serviceflows/1.0.0/le-serviceflows-1.0.0.json"));
    }
}
