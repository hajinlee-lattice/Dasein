package com.latticeengines.proxy.exposed.scoringapi;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResponseErrorHandler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.RemoteLedpException;
import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.DebugRecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.DebugScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.Fields;
import com.latticeengines.domain.exposed.scoringapi.Model;
import com.latticeengines.domain.exposed.scoringapi.ModelDetail;
import com.latticeengines.domain.exposed.scoringapi.ModelType;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("internalScoringApiProxy")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class InternalScoringApiProxy extends BaseRestApiProxy {

    private static class ScoringErrorHandler implements ResponseErrorHandler {
        @Override
        public boolean hasError(ClientHttpResponse response) throws IOException {
            return response.getStatusCode() != HttpStatus.OK;
        }

        @Override
        public void handleError(ClientHttpResponse response) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            IOUtils.copy(response.getBody(), baos);
            String body = new String(baos.toByteArray());
            try {
                new ObjectMapper().readTree(body);
            } catch (Exception e) {
                // Throw a non-LedpException to allow for retries
                throw new RuntimeException(
                        String.format("Received status code %s from scoring api (%s)", response.getStatusCode(), body));
            }
            throw new RemoteLedpException(null, response.getStatusCode(), LedpCode.LEDP_00002, body);
        }
    }

    public InternalScoringApiProxy() {
        super(PropertyUtils.getProperty("common.scoringapi.url"), "/scoreinternal/score");
        setErrorHandler(new ScoringErrorHandler());
    }

    public List<Model> getActiveModels(ModelType type, String tenantIdentifier) {
        String url = null;
        if (type != null) {
            url = constructUrl("/models?type={type}&tenantIdentifier={tenantIdentifier}", type, tenantIdentifier);
        } else {
            url = constructUrl("/models?tenantIdentifier={tenantIdentifier}",tenantIdentifier);
        }

        List<?> resultList = get("getActiveModels", url, List.class);
        List<Model> models = new ArrayList<>();
        if (resultList != null) {

            for (Object obj : resultList) {
                String json = JsonUtils.serialize(obj);
                Model model = JsonUtils.deserialize(json, Model.class);
                models.add(model);
            }
        }
        return models;
    }

    public Fields getModelFields(String modelId, String tenantIdentifier) {
        String url = constructUrl("/models/{modelId}/fields?tenantIdentifier={tenantIdentifier}", modelId,
                tenantIdentifier);
        return get("getModelFields", url, Fields.class);
    }

    public int getModelCount(Date start, boolean considerAllStatus, String tenantIdentifier) {
        String url = "/modeldetails/count?considerAllStatus={considerAllStatus}&tenantIdentifier={tenantIdentifier}";
        if (start != null) {
            String startStr = DateTimeUtils.convertToStringUTCISO8601(start);
            url = constructUrl(url + "&start={start}", considerAllStatus, tenantIdentifier, startStr);
        } else {
            url = constructUrl(url, considerAllStatus, tenantIdentifier);
        }
        return get("getModelCount", url, Integer.class);
    }

    public List<ModelDetail> getPaginatedModels(Date start, boolean considerAllStatus, int offset, int maximum,
            String tenantIdentifier, boolean considerDeleted) {
        String url = "/modeldetails?considerAllStatus={considerAllStatus}&offset={offset}&maximum={maximum}&tenantIdentifier={tenantIdentifier}&considerDeleted={considerDeleted}";
        if (start != null) {
            String startStr = DateTimeUtils.convertToStringUTCISO8601(start);
            url = constructUrl(url + "&start={start}", considerAllStatus, offset, maximum, tenantIdentifier, considerDeleted, startStr);
        } else {
            url = constructUrl(url, considerAllStatus, offset, maximum, tenantIdentifier, considerDeleted);
        }
        List<?> resultList = get("getPaginatedModels", url, List.class);
        List<ModelDetail> paginatedModels = new ArrayList<>();

        if (resultList != null) {

            for (Object obj : resultList) {
                String json = JsonUtils.serialize(obj);
                ModelDetail modelDetail = JsonUtils.deserialize(json, ModelDetail.class);
                paginatedModels.add(modelDetail);
            }
        }
        return paginatedModels;
    }

    public List<ModelDetail> getPaginatedModels(Date start, boolean considerAllStatus, int offset, int maximum,
            String tenantIdentifier) {
        return getPaginatedModels(start, considerAllStatus, offset, maximum, tenantIdentifier, false);
    }

    public ScoreResponse scorePercentileRecord(ScoreRequest scoreRequest, String tenantIdentifier,
            boolean enrichInternalAttributes, boolean performFetchOnlyForMatching) {
        String url = constructUrl("/record?tenantIdentifier={tenantIdentifier}", tenantIdentifier);
        return post("scorePercentileRecord", url, scoreRequest, ScoreResponse.class);
    }

    public List<RecordScoreResponse> scorePercentileRecords(BulkRecordScoreRequest scoreRequest,
            String tenantIdentifier, boolean enrichInternalAttributes, boolean performFetchOnlyForMatching,
            boolean enableMatching) {
        String url = constructUrl(
                "/records?tenantIdentifier={tenantIdentifier}&enrichInternalAttributes={enrichInternalAttributes}&performFetchOnlyForMatching={performFetchOnlyForMatching}&enableMatching={enableMatching}",
                tenantIdentifier, enrichInternalAttributes, performFetchOnlyForMatching, enableMatching);
        List<?> resultList = post("scorePercentileRecords", url, scoreRequest, List.class);
        List<RecordScoreResponse> recordScoreResponseList = new ArrayList<>();
        if (resultList != null) {

            for (Object obj : resultList) {
                String json = JsonUtils.serialize(obj);
                RecordScoreResponse recordScoreResponse = JsonUtils.deserialize(json, RecordScoreResponse.class);
                recordScoreResponseList.add(recordScoreResponse);
            }
        }
        return recordScoreResponseList;
    }

    public List<RecordScoreResponse> scorePercentileAndProbabilityRecords(BulkRecordScoreRequest scoreRequest,
            String tenantIdentifier, boolean enrichInternalAttributes, boolean performFetchOnlyForMatching,
            boolean enableMatching) {
        String url = constructUrl(
                "/records/debug?tenantIdentifier={tenantIdentifier}&enrichInternalAttributes={enrichInternalAttributes}&performFetchOnlyForMatching={performFetchOnlyForMatching}&enableMatching={enableMatching}",
                tenantIdentifier, enrichInternalAttributes, performFetchOnlyForMatching, enableMatching);
        List<?> resultList = post("scorePercentileAndProbabilityRecords", url, scoreRequest, List.class);
        List<RecordScoreResponse> recordScoreResponseList = new ArrayList<>();
        if (resultList != null) {

            for (Object obj : resultList) {
                String json = JsonUtils.serialize(obj);
                DebugRecordScoreResponse recordScoreResponse = JsonUtils.deserialize(json,
                        DebugRecordScoreResponse.class);
                recordScoreResponseList.add(recordScoreResponse);
            }
        }
        return recordScoreResponseList;
    }

    public DebugScoreResponse scoreProbabilityRecord(ScoreRequest scoreRequest, String tenantIdentifier,
            boolean enrichInternalAttributes, boolean performFetchOnlyForMatching) {
        String url = constructUrl("/record/debug?tenantIdentifier={tenantIdentifier}", tenantIdentifier);
        return post("scoreProbabilityRecord", url, scoreRequest, DebugScoreResponse.class);
    }

    public DebugScoreResponse scoreAndEnrichRecordApiConsole(ScoreRequest scoreRequest, String tenantIdentifier,
            boolean enrichInternalAttributes, boolean enforceFuzzyMatch, boolean skipDnBCache) {
        String url = constructUrl(
                "/record/apiconsole/debug?tenantIdentifier={tenantIdentifier}"
                        + "&enrichInternalAttributes={enrichInternalAttributes}"
                        + "&enforceFuzzyMatch={enforceFuzzyMatch}&skipDnBCache={skipDnBCache}",
                tenantIdentifier, enrichInternalAttributes, enforceFuzzyMatch, skipDnBCache);
        return post("scoreProbabilityRecordApiConsole", url, scoreRequest, DebugScoreResponse.class);
    }

}
