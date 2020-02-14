package com.latticeengines.testframework.exposed.proxy.pls;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;

@Component("testProjectProxy")
public class TestProjectProxy extends PlsRestApiProxyBase {

    public TestProjectProxy() {
        super("pls");
    }

    public ProjectDetails createProjectWithProjectId(String displayName, String projectId, Project.ProjectType projectType) {
        List<Object> args = new ArrayList<>();
        args.add(displayName);
        args.add(projectId);
        args.add(projectType.name());
        String urlPattern = "/projects?displayName={displayName}&projectId={projectId}&projectType={projectType}";
        String url = constructUrl(urlPattern, args.toArray(new Object[args.size()]));
        return post("createProject", url, null, ProjectDetails.class);
    }

    public ProjectDetails createProjectWithOutProjectId(String displayName, Project.ProjectType projectType) {
        List<Object> args = new ArrayList<>();
        args.add(displayName);
        args.add(projectType.name());
        String urlPattern = "/projects?displayName={displayName}&projectType={projectType}";
        String url = constructUrl(urlPattern, args.toArray(new Object[args.size()]));
        return post("createProject", url, null, ProjectDetails.class);
    }

    public List<Project> getAllProjects() {
        List<?> raw = get("getAllProjects", constructUrl("/projects/list"), List.class);
        return JsonUtils.convertList(raw, Project.class);
    }

    public ProjectDetails getProjectByProjectId(String projectId) {
        String urlPattern = "/projects?projectId={projectId}";
        String url = constructUrl(urlPattern, projectId);
        return get("getProjectByProjectId", url, null, ProjectDetails.class);
    }

    public void deleteProject(String projectId) {
        String urlPattern = "/projects?projectId={projectId}";
        String url = constructUrl(urlPattern, projectId);
        delete("deleteProject", url);
    }

}
