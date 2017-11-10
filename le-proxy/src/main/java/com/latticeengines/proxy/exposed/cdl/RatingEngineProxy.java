package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("ratingEngineProxy")
public class RatingEngineProxy extends MicroserviceRestApiProxy {

    private static final String URL_PREFIX = "/customerspaces/{customerSpace}/ratingengines";

    protected RatingEngineProxy() {
        super("cdl");
    }

    public List<RatingEngineSummary> getRatingEngineSummaries(String customerSpace) {
        return getRatingEngineSummaries(customerSpace, null, null);
    }

    public List<RatingEngineSummary> getRatingEngineSummaries(String customerSpace, String status, String type) {
        String url = constructUrl(URL_PREFIX, shortenCustomerSpace(customerSpace));
        List<String> params = new ArrayList<>();
        if (StringUtils.isNotBlank(status)) {
            params.add("status=" + status);
        }
        if (StringUtils.isNotBlank(type)) {
            params.add("type=" + type);
        }
        if (!params.isEmpty()) {
            url += "?" + StringUtils.join(params, "&");
        }
        List list = get("get rating engine summary", url, List.class);
        return JsonUtils.convertList(list, RatingEngineSummary.class);
    }

    public RatingEngine getRatingEngine(String customerSpace, String ratingEngineId) {
        String url = constructUrl(URL_PREFIX + "/{ratingEngineId}", shortenCustomerSpace(customerSpace),
                ratingEngineId);
        return get("get rating engine", url, RatingEngine.class);
    }

    public void deleteRatingEngine(String customerSpace, String ratingEngineId) {
        String url = constructUrl(URL_PREFIX + "/{ratingEngineId}", shortenCustomerSpace(customerSpace),
                ratingEngineId);
        delete("delete rating engine", url);
    }

    public RatingEngine createOrUpdateRatingEngine(String customerSpace, RatingEngine ratingEngine) {
        String url = constructUrl(URL_PREFIX, shortenCustomerSpace(customerSpace));
        return post("create rating engine", url, ratingEngine, RatingEngine.class);
    }

    public Set<RatingModel> getRatingModels(String customerSpace, String ratingEngineId) {
        String url = constructUrl(URL_PREFIX + "/{ratingEngineId}/ratingmodels", shortenCustomerSpace(customerSpace),
                ratingEngineId);
        List list = get("get rating models", url, List.class);
        return new HashSet<>(JsonUtils.convertList(list, RatingModel.class));
    }

    public RatingModel getRatingModel(String customerSpace, String ratingEngineId, String ratingModelId) {
        String url = constructUrl(URL_PREFIX + "/{ratingEngineId}/ratingmodels/{ratingModelId}",
                shortenCustomerSpace(customerSpace), ratingEngineId, ratingModelId);
        return get("get rating model", url, RatingModel.class);
    }

    public RatingModel updateRatingModel(String customerSpace, String ratingEngineId,
            String ratingModelId, RatingModel ratingModel) {
        String url = constructUrl(URL_PREFIX + "/{ratingEngineId}/ratingmodels/{ratingModelId}",
                shortenCustomerSpace(customerSpace), ratingEngineId, ratingModelId);
        return post("update rating model", url, ratingModel, RatingModel.class);
    }



}
