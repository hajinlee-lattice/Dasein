package com.latticeengines.proxy.exposed.dante;
//
//import java.util.List;
//
//import org.springframework.stereotype.Component;
//
//import com.latticeengines.domain.exposed.dante.TalkingPointAttribute;
//import com.latticeengines.domain.exposed.dante.TalkingPointNotionAttributes;
//import com.latticeengines.network.exposed.dante.TalkingPointAttributesInterface;
//import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
//
//@Component("talkingPointAttributesProxy ")
//public class TalkingPointsAttributesProxy extends MicroserviceRestApiProxy implements TalkingPointAttributesInterface {
//
//    public TalkingPointsAttributesProxy() {
//        super("/dante/attributes");
//    }
//
//    @SuppressWarnings("unchecked")
//    public List<TalkingPointAttribute> getAccountAttributes(String customerSpace) {
//        String url = constructUrl("/accountattributes?customerSpace=" + customerSpace);
//        return get("getAccountAttributes", url, List.class);
//    }
//
//    @SuppressWarnings("unchecked")
//    public List<TalkingPointAttribute> getRecommendationAttributes(String customerSpace) {
//        String url = constructUrl("/recommendationattributes?customerSpace=" + customerSpace);
//        return get("getRecommendationAttributes", url, List.class);
//    }
//
//    public TalkingPointNotionAttributes getAttributesByNotions(List<String> notions, String customerSpace) {
//        String url = constructUrl("?customerSpace=" + customerSpace);
//        return post("getAttributesByNotions", url, notions, TalkingPointNotionAttributes.class);
//    }
//}
