package com.latticeengines.proxy.exposed.dcp;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayoutDetail;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("enrichmentLayoutProxy")
public class EnrichmentLayoutProxy  extends MicroserviceRestApiProxy implements ProxyInterface {

    protected EnrichmentLayoutProxy () { super("dcp"); }

    private static final String PREFIX = "/customerspaces/{customerSpace}";

    public EnrichmentLayoutDetail getEnrichmentLayoutBySourceId(String customerSpace, String sourceId) {
        String baseUrl = PREFIX + "/enrichmentlayout/sourceId/{sourceId}";
        String url = constructUrl(baseUrl, customerSpace, sourceId);
        return get("Get enrichment layout by sourceId", url, EnrichmentLayoutDetail.class);
    }

    public EnrichmentLayoutDetail getEnrichmentLayoutByLayoutId (String customerSpace, String layoutId) {
        String baseUrl = PREFIX + "/enrichmentlayout/layoutId/{layoutId}";
        String url = constructUrl(baseUrl, customerSpace, layoutId);
        return get("Get enrichment layout by layoutId", url, EnrichmentLayoutDetail.class);
    }

    public List<EnrichmentLayoutDetail> getAll(String customerId) {
        String baseUrl = "customerspaces/{customerSpace}/list";
        String url = getUrl(customerId, baseUrl, null);
        return getList("Get all enrichment layout objects", url, EnrichmentLayoutDetail.class);
    }

    public ResponseDocument<String> create (String customerId, EnrichmentLayout enrichmentLayout) {

        String baseUrl = "/customerspaces/{customerSpace}/enrichmentlayout";
        String url = constructUrl(baseUrl, customerId);
        return post("create enrichmentLayout", url, enrichmentLayout, ResponseDocument.class);
    }

    public ResponseDocument<String> update (String customerId, EnrichmentLayout enrichmentLayout) {
        String baseUrl = "/customerspaces/{customerSpace}/enrichmentlayout";
        String url = constructUrl(baseUrl, customerId);
        return put("update enrichmentLayout", url, enrichmentLayout, ResponseDocument.class);
    }

    public ResponseDocument<String> deleteLayout (String customerId, String layoutId) {
        String baseUrl = "/customerspaces/{customerId}/enrichmentlayout/layoutId/{layoutId}";
        String url = constructUrl(baseUrl, customerId, layoutId);
        delete("delete enrichmentLayout by layoutId", url, String.class);
        return ResponseDocument.successResponse(String.format("EnrichmentLayout with layoutId=%s deleted.", layoutId));
    }

    public ResponseDocument<String> deleteLayoutBySourceId (String customerId, String sourceId) {
        String baseUrl = "/customerspaces/{customerId}/enrichmentlayout/sourceId/{sourceId}";
        String url = constructUrl(baseUrl, customerId, sourceId);
        delete("delete enrichmentLayout by sourceId", url, String.class);
        return ResponseDocument.successResponse(String.format("EnrichmentLayout with sourceId=%s deleted.", sourceId));
    }

    private String getUrl(String customerSpace, String baseUrl, String sourceId) {
        return (null == sourceId) ? constructUrl(baseUrl, customerSpace) : constructUrl(baseUrl, customerSpace, sourceId);
    }
}
