package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.TalkingPointAttribute;
import com.latticeengines.domain.exposed.cdl.TalkingPointNotionAttributes;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

;

@Component("talkingPointAttributesProxy ")
public class TalkingPointsAttributesProxy extends MicroserviceRestApiProxy
        implements ProxyInterface {
    private static final String URL_PREFIX =
            "/customerspaces/{customerSpace}/talkingpoints/attributes";

    public TalkingPointsAttributesProxy() {
        super("cdl");
    }

    @SuppressWarnings("unchecked")
    public List<TalkingPointAttribute> getAccountAttributes(String customerSpace) {
        String url = constructUrl(URL_PREFIX + "/accountattributes",
                shortenCustomerSpace(customerSpace));
        return get("getAccountAttributes", url, List.class);
    }

    @SuppressWarnings("unchecked")
    public List<TalkingPointAttribute> getRecommendationAttributes(String customerSpace) {
        String url = constructUrl(URL_PREFIX + "/recommendationattributes",
                shortenCustomerSpace(customerSpace));
        return get("getRecommendationAttributes", url, List.class);
    }

    public TalkingPointNotionAttributes getAttributesByNotions(String customerSpace,
            List<String> notions) {
        String url = constructUrl(URL_PREFIX, shortenCustomerSpace(customerSpace));
        return post("getAttributesByNotions", url, notions, TalkingPointNotionAttributes.class);
    }
}
