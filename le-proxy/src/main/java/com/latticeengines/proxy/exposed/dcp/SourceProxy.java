package com.latticeengines.proxy.exposed.dcp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceRequest;
import com.latticeengines.domain.exposed.dcp.UpdateSourceRequest;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("sourceProxy")
public class SourceProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    private static final Logger log = LoggerFactory.getLogger(SourceProxy.class);

    protected SourceProxy() {
        super("dcp");
    }

    public Source createSource(String customerSpace, SourceRequest sourceRequest) {
        String baseUrl = "/customerspaces/{customerSpace}/source";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace));

        return post("create dcp source", url, sourceRequest, Source.class);
    }

    public Source updateSource(String customerSpace, UpdateSourceRequest updateSourceRequest) {
        String baseUrl = "/customerspaces/{customerSpace}/source";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace));
        return put("update dcp source", url, updateSourceRequest, Source.class);
    }

    public Source getSource(String customerSpace, String sourceId) {
        String baseUrl = "/customerspaces/{customerSpace}/source/sourceId/{sourceId}";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), sourceId);
        return get("get dcp source by sourceId", url, Source.class);
    }

    public List<Source> getSourceList(String customerSpace, String projectId, int pageIndex, int pageSize) {
        String baseUrl = "/customerspaces/{customerSpace}/source/projectId/{projectId}" +
                "?pageIndex={pageIndex}&{pageSize}={pageSize}";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), projectId, Integer.toString(pageIndex),
                Integer.toString(pageSize));
        List<?> rawResult = get("get dcp source by sourceId", url, List.class);
        return JsonUtils.convertList(rawResult, Source.class);
    }

    public Boolean deleteSource(String customerSpace, String sourceId) {
        String baseUrl = "/customerspaces/{customerSpace}/source/sourceId/{sourceId}";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), sourceId);
        try {
            delete("delete source", url);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    public Boolean pauseSource(String customerSpace, String sourceId) {
        String baseUrl = "/customerspaces/{customerSpace}/source/sourceId/{sourceId}/pause";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), sourceId);
        return put("pause source", url, null, Boolean.class);
    }

    public Boolean reactivateSource(String customerSpace, String sourceId) {
        String baseUrl = "/customerspaces/{customerSpace}/source/sourceId/{sourceId}/reactivate";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), sourceId);
        return put("reactivate source", url, null, Boolean.class);
    }
}
