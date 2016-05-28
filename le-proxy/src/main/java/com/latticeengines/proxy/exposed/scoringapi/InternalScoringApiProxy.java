package com.latticeengines.proxy.exposed.scoringapi;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.DebugScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.Fields;
import com.latticeengines.domain.exposed.scoringapi.Model;
import com.latticeengines.domain.exposed.scoringapi.ModelDetail;
import com.latticeengines.domain.exposed.scoringapi.ModelType;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;
import com.latticeengines.network.exposed.scoringapi.InternalScoringApiInterface;
import com.latticeengines.proxy.exposed.GenericBaseRestApiProxy;

@Component("internalScoringApiProxy")
public class InternalScoringApiProxy extends GenericBaseRestApiProxy implements InternalScoringApiInterface {
    private static final String DATE_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ssZ";
    private static final String UTC = "UTC";
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);

    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone(UTC));
    }

    public InternalScoringApiProxy() {
        super("score");
    }

    @Override
    public List<Model> getActiveModels(String serviceHostPort, ModelType type, String tenantIdentifier) {
        String url = constructUrl(serviceHostPort, "/models/{type}?tenantIdentifier={tenantIdentifier}", type,
                tenantIdentifier);
        System.out.println(url);
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

    @Override
    public Fields getModelFields(String serviceHostPort, String modelId, String tenantIdentifier) {
        String url = constructUrl(serviceHostPort, "/models/{modelId}/fields?tenantIdentifier={tenantIdentifier}",
                modelId, tenantIdentifier);
        return get("getModelFields", url, Fields.class);
    }

    @Override
    public int getModelCount(String serviceHostPort, Date start, boolean considerAllStatus, String tenantIdentifier) {
        String url = "/modeldetails/count?considerAllStatus={considerAllStatus}&tenantIdentifier={tenantIdentifier}";
        if (start != null) {
            String startStr = dateFormat.format(start);
            url = constructUrl(serviceHostPort, url + "&start={start}", considerAllStatus, tenantIdentifier, startStr);
        } else {
            url = constructUrl(serviceHostPort, url, considerAllStatus, tenantIdentifier);
        }
        return get("getModelCount", url, Integer.class);
    }

    @Override
    public List<ModelDetail> getPaginatedModels(String serviceHostPort, Date start, boolean considerAllStatus, int offset, int maximum, String tenantIdentifier) {
        String url = "/modeldetails?considerAllStatus={considerAllStatus}&offset={offset}&maximum={maximum}&tenantIdentifier={tenantIdentifier}";
        if (start != null) {
            String startStr = dateFormat.format(start);
            url = constructUrl(serviceHostPort, url + "&start={start}", considerAllStatus, offset, maximum, tenantIdentifier, startStr);
        } else {
            url = constructUrl(serviceHostPort, url, considerAllStatus, offset, maximum, tenantIdentifier);
        }
        List resultList = get("getPaginatedModels", url, List.class);
        List<ModelDetail> paginatedModels = new ArrayList<>();
        ObjectMapper om = new ObjectMapper();
        if (resultList != null) {

            for (Object obj : resultList) {
                String json = JsonUtils.serialize(obj);
                ModelDetail modelDetail = JsonUtils.deserialize(json, ModelDetail.class);
                paginatedModels.add(modelDetail);
            }
        }
        return paginatedModels;
    }

    @Override
    public ScoreResponse scorePercentileRecord(String serviceHostPort, ScoreRequest scoreRequest,
            String tenantIdentifier) {
        String url = constructUrl(serviceHostPort, "/record?tenantIdentifier={tenantIdentifier}", tenantIdentifier);
        return post("scorePercentileRecord", url, scoreRequest, ScoreResponse.class);
    }

    @Override
    public List<RecordScoreResponse> scorePercentileRecords(String serviceHostPort, BulkRecordScoreRequest scoreRequest,
            String tenantIdentifier) {
        String url = constructUrl(serviceHostPort, "/records?tenantIdentifier={tenantIdentifier}", tenantIdentifier);
        List resultList = post("scorePercentileRecords", url, scoreRequest, List.class);
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

    @Override
    public DebugScoreResponse scoreProbabilityRecord(String serviceHostPort, ScoreRequest scoreRequest,
            String tenantIdentifier) {
        String url = constructUrl(serviceHostPort, "/record/debug?tenantIdentifier={tenantIdentifier}",
                tenantIdentifier);
        return post("scoreProbabilityRecord", url, scoreRequest, DebugScoreResponse.class);
    }

}
