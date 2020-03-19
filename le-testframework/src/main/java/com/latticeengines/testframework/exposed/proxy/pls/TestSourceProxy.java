package com.latticeengines.testframework.exposed.proxy.pls;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceRequest;

@Component("testSourceProxy")
public class TestSourceProxy extends PlsRestApiProxyBase {

    public TestSourceProxy() {
        super("pls/source");
    }

    public Source createSource(SourceRequest sourceRequest) {
        String url = constructUrl("/");
        return post("create source", url, sourceRequest, Source.class);
    }

    public Source getSource(String sourceId) {
        String url = constructUrl("/sourceId/{sourceId}", sourceId);
        return get("get source", url, Source.class);
    }

    public List<Source> getSourcesByProject(String projectId) {
        String url = constructUrl("/projectId/{projectId}", projectId);
        List<?> rawList = get("get source list", url, List.class);
        return JsonUtils.convertList(rawList, Source.class);
    }

    public void deleteSourceById(String sourceId) {
        String url = constructUrl("/sourceId/{sourceId}", sourceId);
        delete("delete source", url);
    }
}
