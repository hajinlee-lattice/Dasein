package com.latticeengines.proxy.lp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;

@Component
public class ModelSummaryProxyImpl extends MicroserviceRestApiProxy implements ModelSummaryProxy {

    protected ModelSummaryProxyImpl() {
        super("lp");
    }

    @Override
    public void setDownloadFlag(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/downloadflag",
                shortenCustomerSpace(customerSpace));
        post("set model summary download flag", url, null);
    }

    @Override
    public ModelSummary getModelSummaryById(String customerSpace, String modelSummaryId) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/{modelSummaryId}",
                shortenCustomerSpace(customerSpace), modelSummaryId);
        return get("set model summary download flag", url, ModelSummary.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean downloadModelSummary(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/downloadmodelsummary",
                shortenCustomerSpace(customerSpace));

        return post("download model summary", url, null, Boolean.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, ModelSummary> getEventToModelSummary(String customerSpace,
            Map<String, String> modelApplicationIdToEventColumn) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/geteventtomodelsummary",
                shortenCustomerSpace(customerSpace));

        Map resObj = post("get event to model summary", url, modelApplicationIdToEventColumn, Map.class);
        Map<String, ModelSummary> res = null;
        if (MapUtils.isNotEmpty(resObj)) {
            res = JsonUtils.convertMap(resObj, String.class, ModelSummary.class);
        }
        return res;
    }
}
