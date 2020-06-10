package com.latticeengines.proxy.lp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelNote;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;

@Component
public class ModelSummaryProxyImpl extends MicroserviceRestApiProxy implements ModelSummaryProxy {

    private static final String LOOKUP_ID_DELIM = "|";

    public ModelSummaryProxyImpl() {
        super("lp");
    }

    @Override
    public void setDownloadFlag(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/downloadflag", customerSpace);
        post("set model summary download flag", url, null);
    }

    @Override
    public boolean downloadModelSummary(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/downloadmodelsummary", customerSpace);

        return post("download model summary", url, null, Boolean.class);
    }

    @Override
    public boolean downloadModelSummary(String customerSpace, Map<String, String> modelApplicationIdToEventColumn) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/downloadmodelsummary", customerSpace);

        return post("download model summary", url, modelApplicationIdToEventColumn, Boolean.class);
    }

    @Override
    public Map<String, ModelSummary> getEventToModelSummary(String customerSpace,
            Map<String, String> modelApplicationIdToEventColumn) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/geteventtomodelsummary",
                customerSpace);

        Map<?, ?> resObj = post("get event to model summary", url, modelApplicationIdToEventColumn, Map.class);
        Map<String, ModelSummary> res = null;
        if (MapUtils.isNotEmpty(resObj)) {
            res = JsonUtils.convertMap(resObj, String.class, ModelSummary.class);
        }

        return res;
    }

    @Override
    public void create(String customerSpace, ModelSummary modelSummary) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/create", customerSpace);
        post("create model summary", url, modelSummary);
    }

    @Override
    public ModelSummary createModelSummary(String customerSpace, ModelSummary modelSummary, boolean usingRaw) {
        String url = String.format("/customerspaces/{customerSpace}/modelsummaries?raw=%s", usingRaw);
        url = constructUrl(url, customerSpace);

        return post("register a model summary", url, modelSummary, ModelSummary.class);
    }

    @Override
    public boolean update(String customerSpace, String modelId, AttributeMap attrMap) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/{modelId}", customerSpace, modelId);

        return put("update a model summary", url, attrMap, Boolean.class);
    }

    @Override
    public boolean updateStatusByModelId(String customerSpace, String modelId, ModelSummaryStatus status) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/updatestatus/{modelId}",
                customerSpace, modelId);

        return put("update a model summary", url, status, Boolean.class);
    }

    @Override
    public boolean deleteByModelId(String customerSpace, String modelId) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/{modelId}", customerSpace, modelId);
        delete("delete model summary by model id", url);

        return true;
    }

    @Override
    public ModelSummary getModelSummary(String customerSpace, String modelId) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/{modelId}", customerSpace, modelId);

        return get("get model summary", url, ModelSummary.class);
    }

    @Override
    public ModelSummary findByModelId(String customerSpace, String modelId,
          boolean returnRelational, boolean returnDocument, boolean validOnly) {
        String url = String.format("/customerspaces/{customerSpace}/modelsummaries/findbymodelid/{modelId}?relational=%s&document=%s&validonly=%s",
                returnRelational, returnDocument, validOnly);
        url = constructUrl(url, customerSpace, modelId);

        return get("find model summary by model id", url, ModelSummary.class);
    }

    @Override
    public ModelSummary findValidByModelId(String customerSpace, String modelId) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/findvalidbymodelId/{modelSummaryId}",
                customerSpace, modelId);

        return get("get a valid model summary by the given model summary id", url, ModelSummary.class);
    }

    @Override
    public ModelSummary getModelSummaryEnrichedByDetails(String customerSpace, String modelId) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/getmodelsummaryenrichedbydetails/{modelId}",
                customerSpace, modelId);

        return get("get model summary enriched by details", url, ModelSummary.class);
    }

    @Override
    public ModelSummary getModelSummaryFromModelId(String customerSpace, String modelId) {
        ModelSummary modelSummary;
        try {
            modelSummary = getModelSummaryEnrichedByDetails(customerSpace, modelId);
            if (modelSummary != null && StringUtils.isEmpty(modelSummary.getEventTableName())) {
                String lookupId = modelSummary.getLookupId();
                String eventTableName = lookupId.substring(lookupId.indexOf(LOOKUP_ID_DELIM) + 1,
                        lookupId.lastIndexOf(LOOKUP_ID_DELIM));
                modelSummary.setEventTableName(eventTableName);
            }
        } catch (Exception e) {
            throw new RuntimeException("getModelSummaryFromModelId: Remote call failure", e);
        }

        return modelSummary;
    }

    @Override
    public ModelSummary findByApplicationId(String customerSpace, String applicationId) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/findbyapplicationid/{applicationId}",
                customerSpace, applicationId);

        return get("find a model summary by application id", url, ModelSummary.class);
    }

    @Override
    public List<ModelSummary> getModelSummaries(String customerSpace, String selection) {
        String baseUrl = "/customerspaces/{customerSpace}/modelsummaries";
        String url = parseOptionalParameter(baseUrl, "selection", selection);
        url = constructUrl(url, customerSpace);

        List<?> res = get("get model summaries", url, List.class);
        return JsonUtils.convertList(res, ModelSummary.class);
    }

    @Override
    public List<ModelSummary> findAll(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/findall", customerSpace);

        List<?> res = get("find all model summaries", url, List.class);
        return JsonUtils.convertList(res, ModelSummary.class);
    }

    @Override
    public List<ModelSummary> findAllValid(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/findallvalid", customerSpace);

        List<?> res = get("find all valid", url, List.class);
        return JsonUtils.convertList(res, ModelSummary.class);
    }

    @Override
    public List<?> findAllActive(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/findallactive", customerSpace);

        return get("find all active", url, List.class);
    }

    @Override
    public List<ModelSummary> findPaginatedModels(String customerSpace, String start,
                                                  boolean considerAllStatus, int offset, int maximum) {
        long lastUpdateTime = 0;
        if (StringUtils.isNotEmpty(start)) {
            lastUpdateTime = DateTimeUtils.convertToLongUTCISO8601(start);
        }

        String url = String.format("/customerspaces/{customerSpace}/modelsummaries/findpaginatedmodels?lastUpdateTime=%d&considerAllStatus=%s&offset=%d&maximum=%s",
                lastUpdateTime, considerAllStatus, offset, maximum);
        url = constructUrl(url, customerSpace);

        List<?> res = get("find paginated models", url, List.class);
        return JsonUtils.convertList(res, ModelSummary.class);
    }

    @Override
    public int findTotalCount(String customerSpace, String start, boolean considerAllStatus) {
        long lastUpdateTime = 0;
        if (StringUtils.isNotEmpty(start)) {
            lastUpdateTime = DateTimeUtils.convertToLongUTCISO8601(start);
        }

        String url = String.format("/customerspaces/{customerSpace}/modelsummaries/findtotalcount?lastUpdateTime=%d&considerAllStatus=%s",
                lastUpdateTime, considerAllStatus);
        url = constructUrl(url, customerSpace);

        return get("find total count", url, Integer.class);
    }

    @Override
    public boolean modelIdinTenant(String customerSpace, String modelId) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/alerts/{modelId}",
                customerSpace, modelId);

        return get("get diagnostic alerts for a model", url, Boolean.class);
    }

    @Override
    public List<Predictor> getAllPredictors(String customerSpace, String modelId) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/predictors/all/{modelId}",
                customerSpace, modelId);

        List<?> res = get("get all the predictors for a specific model", url, List.class);
        return JsonUtils.convertList(res, Predictor.class);
    }

    @Override
    public List<Predictor> getPredictorsForBuyerInsights(String customerSpace, String modelId) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/predictors/bi/{modelId}",
                customerSpace, modelId);

        List<?> res = get("get predictors used by BuyerInsgihts for a specific model", url, List.class);
        return JsonUtils.convertList(res, Predictor.class);
    }

    @Override
    public boolean updatePredictors(String customerSpace, String modelId, AttributeMap attrMap) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelsummaries/predictors/{modelId}",
                customerSpace, modelId);

        return put("update predictors of a sourceModelSummary for the use of BuyerInsights", url, attrMap, Boolean.class);
    }

    @Override
    public List<ModelNote> getAllByModelSummaryId(String customerSpace, String modelId, boolean returnRelational, boolean returnDocument, boolean validOnly) {
        StringBuffer url = new StringBuffer();
        url.append(constructUrl("/customerspaces/{customerSpace}/modelsummaries/modelnote/{modelSummaryId}", customerSpace, modelId));
        List<String> params = new ArrayList<>();
        params.add("relational=" + returnRelational);
        params.add("returnDocument=" + returnDocument);
        params.add("validOnly=" + validOnly);
        url.append("?");
        url.append(StringUtils.join(params, "&"));
        List<?> res = get("get model notes by model id", url.toString(), List.class);
        return JsonUtils.convertList(res, ModelNote.class);
    }

    /** Internal */
    @Override
    public ModelSummary getByModelId(String modelId) {
        String url = constructUrl("/modelsummaries/internal/getmodelsummarybymodelid/{modelSummaryId}", modelId);

        return get("get a model summary by the given model summary id", url, ModelSummary.class);
    }

    @Override
    public ModelSummary retrieveByModelIdForInternalOperations(String modelId) {
        String url = constructUrl("/modelsummaries/internal/retrievebymodelidforinternaloperations/{modelId}", modelId);

        return get("retrieve by model id for internal operations", url, ModelSummary.class);
    }

    @Override
    public List<ModelSummary> getAllForTenant(String tenantName) {
        String url = constructUrl("/modelsummaries/internal/tenant/{tenantName}", tenantName);

        List<?> res = get("get list of model summaries available for given tenant", url, List.class);
        return JsonUtils.convertList(res, ModelSummary.class);
    }

    @Override
    public List<ModelSummary> getModelSummariesByApplicationId(String applicationId) {
        String url = constructUrl("/modelsummaries/internal/getmodelsummariesbyapplicationid/{applicationId}", applicationId);

        List<?> res = get("get model summaries", url, List.class);
        return JsonUtils.convertList(res, ModelSummary.class);
    }

    @Override
    public List<ModelSummary> getModelSummariesModifiedWithinTimeFrame(long timeFrame) {
        String url = constructUrl("/modelsummaries/internal/updated/{timeframe}", timeFrame);

        List<?> res = get("get model summaries updated within time frame", url, List.class);
        return JsonUtils.convertList(res, ModelSummary.class);
    }

    @Override
    public void deleteById(String id) {
        String url = constructUrl("/modelsummaries/internal/modelnote/{modelSummaryId}", id);
        delete("delete model notes by model id", url);
    }

    @Override
    public void updateById(String id, NoteParams noteParams) {
        String url = constructUrl("/modelsummaries/internal/modelnote/{modelSummaryId}", id);
        put("update model notes by model id", url, noteParams);
    }

    @Override
    public void create(String modelSummaryId, NoteParams noteParams) {
        String url = constructUrl("/modelsummaries/internal/modelnote/{modelSummaryId}", modelSummaryId);
        post("create model notes by model id", url, noteParams);
    }

    @Override
    public void copyNotes(String sourceModelSummaryId, String targetModelSummaryId) {
        StringBuffer url = new StringBuffer();
        url.append(constructUrl("/modelsummaries/internal/modelnote/copy/{modelSummaryId}", sourceModelSummaryId));
        List<String> params = new ArrayList<>();
        params.add("targetModelSummaryId=" + targetModelSummaryId);
        url.append("?");
        url.append(StringUtils.join(params, "&"));
        post("copy model notes", url.toString(), null);
    }

    private String parseOptionalParameter(String baseUrl, String parameterName, String parameterValue) {
        if (StringUtils.isNotEmpty(parameterValue)) {
            return String.format(baseUrl + "?%s=%s", parameterName, parameterValue);
        } else {
            return baseUrl;
        }
    }
}
