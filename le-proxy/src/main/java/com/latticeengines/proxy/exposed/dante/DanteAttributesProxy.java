package com.latticeengines.proxy.exposed.dante;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dante.DanteAttribute;
import com.latticeengines.domain.exposed.dante.DanteNotionAttributes;
import com.latticeengines.network.exposed.dante.DanteAttributesInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("danteAttributesProxy ")
public class DanteAttributesProxy extends MicroserviceRestApiProxy implements DanteAttributesInterface {

    public DanteAttributesProxy() {
        super("/dante/attributes");
    }

    @SuppressWarnings("unchecked")
    public List<DanteAttribute> getAccountAttributes(String customerSpace) {
        String url = constructUrl("/accountattributes?customerSpace=" + customerSpace);
        return get("getAccountAttributes", url, List.class);
    }

    @SuppressWarnings("unchecked")
    public List<DanteAttribute> getRecommendationAttributes(String customerSpace) {
        String url = constructUrl("/recommendationattributes?customerSpace=" + customerSpace);
        return get("getRecommendationAttributes", url, List.class);
    }

    @SuppressWarnings("unchecked")
    public DanteNotionAttributes getAttributesByNotions(List<String> notions, String customerSpace) {
        String url = constructUrl("?customerSpace=" + customerSpace);
        return post("getAttributesByNotions", url, notions, DanteNotionAttributes.class);
    }
}
