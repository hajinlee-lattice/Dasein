package com.latticeengines.testframework.exposed.proxy.pls;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectRequest;
import com.latticeengines.domain.exposed.dcp.ProjectSummary;

@Component("testProjectProxy")
public class TestProjectProxy extends PlsRestApiProxyBase {

    public TestProjectProxy() {
        super("pls/projects");
    }

    public ProjectDetails createProjectWithProjectId(String displayName, String projectId, Project.ProjectType projectType) {
        ProjectRequest request = new ProjectRequest();
        request.setDisplayName(displayName);
        request.setProjectId(projectId);
        request.setProjectType(projectType);
        String url = constructUrl("/");
        return post("createProject", url, request, ProjectDetails.class);
    }

    public ProjectDetails createProjectWithOutProjectId(String displayName, Project.ProjectType projectType) {
        ProjectRequest request = new ProjectRequest();
        request.setDisplayName(displayName);
        request.setProjectType(projectType);
        String url = constructUrl("/");
        return post("createProject", url, request, ProjectDetails.class);
    }

    public List<ProjectSummary> getAllProjects(Boolean includeSources, Boolean includeArchived) {
        String url = constructUrl("/list");
        boolean isFirst = true;
        if (includeSources) {
            url += "?includeSources=true";
            isFirst = false;
        }
        if (includeArchived) {
            url += isFirst ? "?includeArchived=true" : "&includeArchived=true";
        }
        List<?> raw = get("getAllProjects", url, List.class);
        return JsonUtils.convertList(raw, ProjectSummary.class);
    }

    public ProjectDetails getProjectByProjectId(String projectId) {
        String urlPattern = "/projectId/{projectId}";
        String url = constructUrl(urlPattern, projectId);
        return get("getProjectByProjectId", url, null, ProjectDetails.class);
    }

    public void deleteProject(String projectId) {
        String urlPattern = "/{projectId}";
        String url = constructUrl(urlPattern, projectId);
        delete("deleteProject", url);
    }

    public GrantDropBoxAccessResponse getDropFolderAccessByProjectId(String projectId) {
        String urlPattern = "/projectId/{projectId}/dropFolderAccess";
        String url = constructUrl(urlPattern, projectId);
        return get("getDropFolderAccessByProjectId", url, null, GrantDropBoxAccessResponse.class);
    }

}
