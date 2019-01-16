package com.latticeengines.eai.functionalframework;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.yarn.functionalframework.YarnMiniClusterFunctionalTestNGBase;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-eai-context.xml" })
public class EaiMiniClusterFunctionalTestNGBase extends YarnMiniClusterFunctionalTestNGBase
        implements EaiFunctionalTestNGInterface {

    @Override
    protected void uploadArtifactsToHdfs() throws IOException {
        super.uploadArtifactsToHdfs();
        String eaiHdfsPath = String.format("%s/eai", manifestService.getLedpPath());
        FileUtils.deleteDirectory(new File("eai"));
        HdfsUtils.copyHdfsToLocal(yarnConfiguration, eaiHdfsPath, ".");
        HdfsUtils.copyFromLocalToHdfs(miniclusterConfiguration, "eai", eaiHdfsPath);

    }
}
