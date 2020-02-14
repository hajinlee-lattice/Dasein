package com.latticeengines.proxy.exposed.dcp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("dcpProxy")
public class ProjectProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    protected ProjectProxy() {
        super("dcp");
    }

    public ProjectProxy(String hostPort) {
        super(hostPort, "dcp");
    }

    public ProjectDetails createDCPProject(String customerSpace, String projectId, String displayName, Project.ProjectType projectType, String user) {
        String baseUrl = "/customerspaces/{customerSpace}/project?displayName={displayName}&user={user}";
        List<String> args = new ArrayList<>();
        args.add(shortenCustomerSpace(customerSpace));
        args.add(displayName);
        args.add(user);
        if (StringUtils.isNotBlank(projectId)) {
            baseUrl += "&projectId={projectId}";
            args.add(projectId);
        }

        String url = constructUrl(baseUrl, args.toArray());
        String json =  post("create dcp project", url, projectType, String.class);
        ResponseDocument<ProjectDetails> responseDoc = ResponseDocument.generateFromJSON(json, ProjectDetails.class);
        if (responseDoc == null) {
            throw new RuntimeException("Failed to create DCP project!");
        }
        if (!responseDoc.isSuccess()) {
            throw new LedpException(LedpCode.LEDP_18216, responseDoc.getErrors().toArray());
        }
        return responseDoc.getResult();
    }

    public List<Project> getAllDCPProject(String customerSpace) {
        String url = "/customerspaces/{customerSpace}/project/list";
        url = constructUrl(url, customerSpace);
        List<?> results = get("get all dcp project", url, List.class);
        return JsonUtils.convertList(results, Project.class);
    }

    public ProjectDetails getDCPProjectByProjectId(String customerSpace, String projectId) {
        String url = "/customerspaces/{customerSpace}/project?projectId={projectId}";
        url = constructUrl(url, customerSpace, projectId);
        return get("get dcp project by projectId", url, ProjectDetails.class);
    }

    public void deleteProject(String customerSpace, String projectId) {
        String url = "/customerspaces/{customerSpace}/project?projectId={projectId}";
        url = constructUrl(url, customerSpace, projectId);
        delete("delete dcp project by projectId", url);
    }
}
