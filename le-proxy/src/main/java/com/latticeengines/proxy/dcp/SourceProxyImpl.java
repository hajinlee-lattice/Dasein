package com.latticeengines.proxy.dcp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceRequest;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.dcp.SourceProxy;

@Component("sourceProxy")
public class SourceProxyImpl extends MicroserviceRestApiProxy implements SourceProxy {

    private static final Logger log = LoggerFactory.getLogger(SourceProxyImpl.class);

    protected SourceProxyImpl() {
        super("dcp");
    }

    @Override
    public Source createSource(String customerSpace, SourceRequest sourceRequest) {
        String baseUrl = "/customerspaces/{customerSpace}/source";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace));

        return post("create dcp source", url, sourceRequest, Source.class);
    }

    @Override
    public Source getSource(String customerSpace, String sourceId) {
        String baseUrl = "/customerspaces/{customerSpace}/source/sourceId/{sourceId}";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), sourceId);
        return get("get dcp source by sourceId", url, Source.class);
    }

    @Override
    public List<Source> getSourceList(String customerSpace, String projectId) {
        String baseUrl = "/customerspaces/{customerSpace}/source/projectId/{projectId}";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), projectId);
        List<?> rawResult = get("get dcp source by sourceId", url, List.class);
        return JsonUtils.convertList(rawResult, Source.class);
    }

    @Override
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

    @Override
    public Boolean pauseSource(String customerSpace, String sourceId) {
        String baseUrl = "/customerspaces/{customerSpace}/source/sourceId/{sourceId}/pause";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), sourceId);
        try {
            put("pause source", url);
            return Boolean.TRUE;
        } catch (RuntimeException e) {
            return Boolean.FALSE;
        }
    }
}
