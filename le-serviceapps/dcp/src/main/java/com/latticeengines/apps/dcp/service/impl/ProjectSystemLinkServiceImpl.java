package com.latticeengines.apps.dcp.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.dcp.entitymgr.ProjectSystemLinkEntityMgr;
import com.latticeengines.apps.dcp.service.ProjectService;
import com.latticeengines.apps.dcp.service.ProjectSystemLinkService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectSystemLink;

@Service("projectSystemLinkService")
public class ProjectSystemLinkServiceImpl implements ProjectSystemLinkService {

    @Inject
    private ProjectSystemLinkEntityMgr projectSystemLinkEntityMgr;

    @Inject
    private ProjectService projectService;

    @Override
    public void createLink(String customerSpace, Project project, S3ImportSystem importSystem) {
        Preconditions.checkNotNull(project);
        Preconditions.checkNotNull(importSystem);
        ProjectSystemLink link = new ProjectSystemLink();
        link.setTenant(MultiTenantContext.getTenant());
        link.setProject(project);
        link.setImportSystem(importSystem);
        projectSystemLinkEntityMgr.create(link);
    }

    @Override
    public void createLink(String customerSpace, String projectId, S3ImportSystem importSystem) {
        Project project = projectService.getProjectByProjectId(customerSpace, projectId);
        createLink(customerSpace, project, importSystem);
    }
}
