package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("cdlDependenciesProxy")
public class CDLDependenciesProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    protected CDLDependenciesProxy() {
        super("cdl");
    }

    public List<MetadataSegment> getDependingSegments(String customerSpace, List<String> attributes) {
        String url = constructUrl("/customerspaces/{customerSpace}/dependencies/segments",
                shortenCustomerSpace(customerSpace));
        List<?> rawSegments = post("get depending segments", url, attributes, List.class);
        return JsonUtils.convertList(rawSegments, MetadataSegment.class);
    }

    public List<RatingModel> getDependingRatingModels(String customerSpace, List<String> attributes) {
        String url = constructUrl("/customerspaces/{customerSpace}/dependencies/ratingmodels",
                shortenCustomerSpace(customerSpace));
        List<?> rawSegments = post("get depending rating models", url, attributes, List.class);
        return JsonUtils.convertList(rawSegments, RatingModel.class);
    }

    public List<RatingEngine> getDependingRatingEngines(String customerSpace, List<String> attributes) {
        String url = constructUrl("/customerspaces/{customerSpace}/dependencies/ratingengines",
                shortenCustomerSpace(customerSpace));
        List<?> rawSegments = post("get depending rating engines", url, attributes, List.class);
        return JsonUtils.convertList(rawSegments, RatingEngine.class);
    }

    public List<Play> getDependingPlays(String customerSpace, List<String> attributes) {
        String url = constructUrl("/customerspaces/{customerSpace}/dependencies/plays",
                shortenCustomerSpace(customerSpace));
        List<?> rawSegments = post("get depending plays", url, attributes, List.class);
        return JsonUtils.convertList(rawSegments, Play.class);
    }

    public List<Play> getDependantPlays(String customerSpace, List<String> attributes) {
        String url = constructUrl("/customerspaces/{customerSpace}/dependencies/plays",
                shortenCustomerSpace(customerSpace));
        List<?> rawSegments = post("Get dependant play display names", url, attributes, List.class);
        return JsonUtils.convertList(rawSegments, Play.class);
    }

}
