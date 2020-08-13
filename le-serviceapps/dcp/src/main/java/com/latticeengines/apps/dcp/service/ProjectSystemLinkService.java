package com.latticeengines.apps.dcp.service;

import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.dcp.Project;

public interface ProjectSystemLinkService {

    void createLink(String customerSpace, Project project, S3ImportSystem importSystem);

    void createLink(String customerSpace, String projectId, S3ImportSystem importSystem);
}
