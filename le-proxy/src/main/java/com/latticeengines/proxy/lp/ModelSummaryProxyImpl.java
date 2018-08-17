package com.latticeengines.proxy.lp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
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
    public ModelSummary getModelSummaryByModelId(String customerSpace, String modelId) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/getmodelsummarybymodelid/{modelSummaryId}",
                shortenCustomerSpace(customerSpace), modelId);
        return get("get model summary by model id", url, ModelSummary.class);
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
    public boolean downloadModelSummary(String customerSpace, Map<String, String> modelApplicationIdToEventColumn) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/downloadmodelsummary",
                shortenCustomerSpace(customerSpace));

        return post("download model summary", url, modelApplicationIdToEventColumn, Boolean.class);
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

    @Override
    @SuppressWarnings("unchecked")
    public List<ModelSummary> getModelSummaries(String customerSpace, String selection) {
        String baseUrl = "/customerspaces/{customerSpace}/modelsummaries";
        String url = parseOptionalParameter(baseUrl, "selection", selection);
        url = constructUrl(url, shortenCustomerSpace(customerSpace));

        List<?> res = get("get model summaries", url, List.class);
        return JsonUtils.convertList(res, ModelSummary.class);

    }

    String parseOptionalParameter(String baseUrl, String parameterName, String parameterValue) {
        if (StringUtils.isNotEmpty(parameterValue)) {
            return String.format(baseUrl + "?%s=%s", parameterName, parameterValue);
        } else {
            return baseUrl;
        }
    }
}
