package com.latticeengines.proxy.exposed.dante;

import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.network.exposed.dante.DanteAttributesInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("danteAttributesProxy ")
public class DanteAttributesProxy extends MicroserviceRestApiProxy implements DanteAttributesInterface {

    public DanteAttributesProxy() {
        super("/dante/attributes");
    }

    @SuppressWarnings("unchecked")
    public ResponseDocument<Map<String, String>> getAccountAttributes(String customerSpace) {
        String url = constructUrl("/accountattributes");
        return get("getAccountAttributes", url, ResponseDocument.class);
    }

    @SuppressWarnings("unchecked")
    public ResponseDocument<Map<String, String>> getRecommendationAttributes(String customerSpace) {
        String url = constructUrl("/recommendationattributes");
        return get("getRecommendationAttributes", url, ResponseDocument.class);
    }
}
