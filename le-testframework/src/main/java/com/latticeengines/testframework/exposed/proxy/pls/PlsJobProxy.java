package com.latticeengines.testframework.exposed.proxy.pls;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.workflow.Job;

@Component("plsJobProxy")
public class PlsJobProxy extends PlsRestApiProxyBase {
    private static final String ROOT_PATH = "pls/jobs";

    public PlsJobProxy() {
        super(ROOT_PATH);
    }

    public PlsJobProxy(String hostport) {
        super(hostport, ROOT_PATH);
    }

    public List<Job> getAllJobs() {
        List<?> rawList = get("get all jobs", constructUrl(), List.class);
        return JsonUtils.convertList(rawList, Job.class);
    }
}
