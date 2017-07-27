package com.latticeengines.proxy.exposed.dante;

import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.network.exposed.dante.DanteAttributesInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("danteAttributesProxy ")
public class DanteAttributesProxy extends MicroserviceRestApiProxy implements DanteAttributesInterface {

    public DanteAttributesProxy() {
        super("/dante/attributes");
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> getAccountAttributes(String customerSpace) {
        String url = constructUrl("/accountattributes?customerSpace=" + customerSpace);
        return get("getAccountAttributes", url, Map.class);
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> getRecommendationAttributes(String customerSpace) {
        String url = constructUrl("/recommendationattributes?customerSpace=" + customerSpace);
        return get("getRecommendationAttributes", url, Map.class);
    }
}
