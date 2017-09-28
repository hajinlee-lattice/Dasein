package com.latticeengines.testframework.exposed.service;

import java.io.File;
import java.io.InputStream;

public interface TestArtifactService {

    InputStream readTestArtifactAsStream(String objectDir, String version, String fileName);
    File downloadTestArtifact(String objectDir, String version, String fileName);

}
