package com.latticeengines.proxy.exposed.dcp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectRequest;
import com.latticeengines.domain.exposed.dcp.ProjectSummary;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("projectProxy")
public class ProjectProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    private static final Logger log = LoggerFactory.getLogger(UploadProxy.class);

    protected ProjectProxy() {
        super("dcp");
    }

    public ProjectProxy(String hostPort) {
        super(hostPort, "dcp");
    }

    public ProjectDetails createDCPProject(String customerSpace, ProjectRequest projectRequest, String user) {
        String baseUrl = "/customerspaces/{customerSpace}/project?user={user}";
        List<String> args = new ArrayList<>();
        args.add(shortenCustomerSpace(customerSpace));
        args.add(user);

        String url = constructUrl(baseUrl, args.toArray());
        String json =  post("create dcp project", url, projectRequest, String.class);
        ResponseDocument<ProjectDetails> responseDoc = ResponseDocument.generateFromJSON(json, ProjectDetails.class);
        if (responseDoc == null) {
            throw new RuntimeException("Failed to create DCP project!");
        }
        if (!responseDoc.isSuccess()) {
            throw new LedpException(LedpCode.LEDP_18216, responseDoc.getErrors().toArray());
        }
        return responseDoc.getResult();
    }

    public List<ProjectSummary> getAllDCPProject(String customerSpace, Boolean includeSources, Boolean includeArchived, int pageIndex,
                                                 int pageSize, List<String> teamIds) {
        String url = "/customerspaces/{customerSpace}/project/list?includeSources={includeSources}" +
                "&pageIndex={pageIndex}&pageSize={pageSize}&includeArchived={includeArchived}";
        url = constructUrl(url, customerSpace, includeSources.toString(), Integer.toString(pageIndex),
                Integer.toString(pageSize), includeArchived.toString());
        List<?> results = post("get all dcp project", url, teamIds, List.class);
        return JsonUtils.convertList(results, ProjectSummary.class);
    }

    public ProjectDetails getDCPProjectByProjectId(String customerSpace, String projectId, Boolean includeSources, List<String> teamIds) {
        String url = "/customerspaces/{customerSpace}/project/projectId/{projectId}?includeSources={includeSources}";
        url = constructUrl(url, customerSpace, projectId, includeSources.toString());
        return post("get dcp project by projectId", url, teamIds, ProjectDetails.class);
    }

    public void deleteProject(String customerSpace, String projectId, List<String> teamIds) {
        String url = "/customerspaces/{customerSpace}/project/{projectId}";
        url = constructUrl(url, customerSpace, projectId);
        delete("delete dcp project by projectId", url, teamIds);
    }

    public GrantDropBoxAccessResponse getDropFolderAccessByProjectId(String customerSpace, String projectId, List<String> teamIds) {
        String url = "/customerspaces/{customerSpace}/project/projectId/{projectId}/dropFolderAccess";
        url = constructUrl(url, customerSpace, projectId);
        return get("get dropFolderAccess by projectId", url, GrantDropBoxAccessResponse.class);
    }

    public void updateTeamId(String customerSpace, String projectId, String teamId) {
        String baseUrl = "/customerspaces/{customerSpace}/project/projectId/{projectId}/teamId/{teamId}";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), projectId, teamId);
        put("update teamId", url);
    }
}
