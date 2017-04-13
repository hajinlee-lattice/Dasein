package com.latticeengines.eai.functionalframework;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.functionalframework.DataplatformMiniClusterFunctionalTestNG;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-eai-context.xml" })
public class EaiMiniClusterFunctionalTestNGBase extends DataplatformMiniClusterFunctionalTestNG
        implements EaiFunctionalTestNGInterface {

    @Override
    protected void uploadArtifactsToHdfs() throws IOException {
        super.uploadArtifactsToHdfs();
        String eaiHdfsPath = String.format("/app/%s/eai", versionManager.getCurrentVersionInStack(stackName))
                .toString();
        FileUtils.deleteDirectory(new File("eai"));
        HdfsUtils.copyHdfsToLocal(yarnConfiguration, eaiHdfsPath, ".");
        HdfsUtils.copyFromLocalToHdfs(miniclusterConfiguration, "eai", eaiHdfsPath);

    }
}
