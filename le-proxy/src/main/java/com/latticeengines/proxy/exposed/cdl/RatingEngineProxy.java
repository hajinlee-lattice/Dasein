package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingModelWithPublishedHistoryDTO;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

import io.swagger.annotations.ApiOperation;

@Component("ratingEngineProxy")
public class RatingEngineProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    private static final String URL_PARAM_TEMPLATE = "&%s={%s}";
    private static final String URL_PREFIX = "/customerspaces/{customerSpace}/ratingengines";

    protected RatingEngineProxy() {
        super("cdl");
    }

    public List<RatingEngineSummary> getRatingEngineSummaries(String customerSpace) {
        return getRatingEngineSummaries(customerSpace, null, null);
    }

    public List<RatingEngineSummary> getRatingEngineSummaries(String customerSpace, String status,
            String type) {
        return getRatingEngineSummaries(customerSpace, status, type, false);
    }

    @SuppressWarnings("rawtypes")
    public List<RatingEngineSummary> getRatingEngineSummaries(String customerSpace, String status,
            String type, boolean publishedRatingsOnly) {
        String url = constructUrl(URL_PREFIX, shortenCustomerSpace(customerSpace));
        List<String> params = new ArrayList<>();
        if (StringUtils.isNotBlank(status)) {
            params.add("status=" + status);
        }
        if (StringUtils.isNotBlank(type)) {
            params.add("type=" + type);
        }
        if (publishedRatingsOnly) {
            params.add("publishedratingsonly=true");
        }
        if (!params.isEmpty()) {
            url += "?" + StringUtils.join(params, "&");
        }
        List list = get("get rating engine summary", url, List.class);
        return JsonUtils.convertList(list, RatingEngineSummary.class);
    }

    @SuppressWarnings("rawtypes")
    public List<RatingEngine> getAllDeletedRatingEngines(String customerSpace) {
        String url = constructUrl(URL_PREFIX + "/deleted", shortenCustomerSpace(customerSpace));
        List list = get("get all deleted rating engines", url, List.class);
        return JsonUtils.convertList(list, RatingEngine.class);
    }

    public RatingEngine getRatingEngine(String customerSpace, String ratingEngineId) {
        String url = constructUrl(URL_PREFIX + "/{ratingEngineId}",
                shortenCustomerSpace(customerSpace), ratingEngineId);
        return get("get rating engine", url, RatingEngine.class);
    }

    public void deleteRatingEngine(String customerSpace, String ratingEngineId) {
        String url = constructUrl(URL_PREFIX + "/{ratingEngineId}",
                shortenCustomerSpace(customerSpace), ratingEngineId);
        delete("delete rating engine", url);
    }

    public void deleteRatingEngine(String customerSpace, String ratingEngineId, Boolean hardDelete,
            String actionInitiator) {
        String url = constructUrl(URL_PREFIX + "/{ratingEngineId}",
                shortenCustomerSpace(customerSpace), ratingEngineId);
        if (hardDelete != null) {
            url += String.format("?hard-delete=%s", hardDelete.toString());
        }
        if (actionInitiator != null) {
            String param = hardDelete != null ? "&" : "?";
            url += String.format("%saction-initiator=%s", param, actionInitiator);
        }
        delete("delete rating engine", url);
    }

    public void revertDeleteRatingEngine(String customerSpace, String ratingEngineId) {
        String url = constructUrl(URL_PREFIX + "/{ratingEngineId}/revertdelete",
                shortenCustomerSpace(customerSpace), ratingEngineId);
        put("revert delete rating engine", url);
    }

    public RatingEngine createOrUpdateRatingEngine(String customerSpace, RatingEngine ratingEngine,
            String user, Boolean unlinkSegment, Boolean createAction) {
        String url = constructCreateOrUpdateRatingEngineUrl(customerSpace, user, unlinkSegment,
                createAction);
        return post("create rating engine", url, ratingEngine, RatingEngine.class);
    }

    public RatingEngine createOrUpdateRatingEngine(String customerSpace,
            RatingEngine ratingEngine) {
        return createOrUpdateRatingEngine(customerSpace, ratingEngine, null, false, true);
    }

    public RatingEngine createOrUpdateRatingEngine(String customerSpace, RatingEngine ratingEngine,
            Boolean unlinkSegment) {
        return createOrUpdateRatingEngine(customerSpace, ratingEngine, null, unlinkSegment, true);
    }

    @SuppressWarnings("rawtypes")
    public List<RatingModel> getRatingModels(String customerSpace, String ratingEngineId) {
        String url = constructUrl(URL_PREFIX + "/{ratingEngineId}/ratingmodels",
                shortenCustomerSpace(customerSpace), ratingEngineId);
        List list = get("get rating models", url, List.class);
        return JsonUtils.convertList(list, RatingModel.class);
    }

    public RatingModel createModelIteration(String customerSpace, String ratingEngineId,
            RatingModel ratingModel) {
        String url = constructUrl(URL_PREFIX + "/{ratingEngineId}/ratingmodels",
                shortenCustomerSpace(customerSpace), ratingEngineId);
        return post("create model iteration", url, ratingModel, ratingModel.getClass());
    }

    public RatingModel getRatingModel(String customerSpace, String ratingEngineId,
            String ratingModelId) {
        String url = constructUrl(URL_PREFIX + "/{ratingEngineId}/ratingmodels/{ratingModelId}",
                shortenCustomerSpace(customerSpace), ratingEngineId, ratingModelId);
        return get("get rating model", url, RatingModel.class);
    }

    public RatingModel updateRatingModel(String customerSpace, String ratingEngineId,
            String ratingModelId, RatingModel ratingModel, String user) {
        String url = constructUpdateRatingModelUrl(customerSpace, ratingEngineId, ratingModelId,
                user);
        return post("update rating model", url, ratingModel, ratingModel.getClass());
    }

    public RatingModel updateRatingModel(String customerSpace, String ratingEngineId,
            String ratingModelId, RatingModel ratingModel) {
        return updateRatingModel(customerSpace, ratingEngineId, ratingModelId, ratingModel, null);
    }

    @SuppressWarnings("unchecked")
    @Deprecated
    public Map<String, List<ColumnMetadata>> getIterationAttributes(String customerSpace,
            String ratingEngineId, String ratingModelId, String dataStores) {
        String url = constructUrl(
                URL_PREFIX + "/{ratingEngineId}/ratingmodels/{ratingModelId}/attributes",
                shortenCustomerSpace(customerSpace), ratingEngineId, ratingModelId)
                + (StringUtils.isNotEmpty(dataStores) ? "?data_stores=" + dataStores : "");
        return JsonUtils.convertMapWithListValue(
                get("get metadata for a rating model", url, Map.class), String.class,
                ColumnMetadata.class);
    }

    @SuppressWarnings("unchecked")
    public List<ColumnMetadata> getIterationMetadata(String customerSpace, String ratingEngineId,
            String ratingModelId, String dataStores) {
        String url = constructUrl(
                URL_PREFIX + "/{ratingEngineId}/ratingmodels/{ratingModelId}/metadata",
                shortenCustomerSpace(customerSpace), ratingEngineId, ratingModelId)
                + (StringUtils.isNotEmpty(dataStores) ? "?data_stores=" + dataStores : "");
        return JsonUtils.convertList(get("get metadata for a rating model", url, List.class),
                ColumnMetadata.class);
    }

    public Map<String, StatsCube> getIterationMetadataCube(String customerSpace,
            String ratingEngineId, String ratingModelId, String dataStores) {
        String url = constructUrl(
                URL_PREFIX + "/{ratingEngineId}/ratingmodels/{ratingModelId}/metadata/cube",
                shortenCustomerSpace(customerSpace), ratingEngineId, ratingModelId)
                + (StringUtils.isNotEmpty(dataStores) ? "?data_stores=" + dataStores : "");
        return JsonUtils.convertMap(get("get metadata cube for a rating model", url, Map.class),
                String.class, StatsCube.class);
    }

    public TopNTree getIterationMetadataTopN(String customerSpace, String ratingEngineId,
            String ratingModelId, String dataStores) {
        String url = constructUrl(
                URL_PREFIX + "/{ratingEngineId}/ratingmodels/{ratingModelId}/metadata/topn",
                shortenCustomerSpace(customerSpace), ratingEngineId, ratingModelId)
                + (StringUtils.isNotEmpty(dataStores) ? "?data_stores=" + dataStores : "");
        return JsonUtils.convertValue(
                get("get metadata TopNTree for a rating model", url, Map.class), TopNTree.class);
    }

    public void setScoringIteration(String customerSpace, String ratingEngineId,
            String ratingModelId, List<BucketMetadata> bucketMetadatas, String userEmail) {
        String url = constructUrl(URL_PREFIX
                + "/{ratingEngineId}/ratingmodels/{ratingModel}/setScoringIteration/?useremail={userEmail}",
                shortenCustomerSpace(customerSpace), ratingEngineId, ratingModelId, userEmail);
        post("setScoringIteration", url, bucketMetadatas, Object.class);
    }

    @SuppressWarnings("rawtypes")
    public List<String> getRatingEngineIdsInSegment(String customerSpace, String segment) {
        String url = constructUrl(URL_PREFIX + "/ids?segment={segment}",
                shortenCustomerSpace(customerSpace), segment);
        List list = get("get rating engine ids in segment", url, List.class);
        return JsonUtils.convertList(list, String.class);
    }

    @SuppressWarnings("rawtypes")
    public List<String> getRatingEngineIds(String customerSpace) {
        String url = constructUrl(URL_PREFIX + "/ids", shortenCustomerSpace(customerSpace));
        List list = get("get rating engine ids", url, List.class);
        return JsonUtils.convertList(list, String.class);
    }

    @SuppressWarnings("rawtypes")
    public Map<String, Long> updateRatingEngineCounts(String customerSpace, String ratingEngineId) {
        String url = constructUrl(URL_PREFIX + "/{ratingEngineId}/counts",
                shortenCustomerSpace(customerSpace), ratingEngineId);
        Map map = put("update rating engine counts", url, null, Map.class);
        return JsonUtils.convertMap(map, String.class, Long.class);
    }

    public List<RatingModelWithPublishedHistoryDTO> getPublishedHistory(
            @PathVariable String customerSpace, @PathVariable String ratingEngineId) {
        String url = constructUrl(URL_PREFIX + "/{ratingEngineId}/publishedhistory",
                shortenCustomerSpace(customerSpace), ratingEngineId);
        return getList("get published history for given rating engine Id", url,
                RatingModelWithPublishedHistoryDTO.class);
    }

    @SuppressWarnings("rawtypes")
    public List<RatingEngineNote> getAllNotes(String customerSpace, String ratingEngineId) {
        String url = constructUrl(URL_PREFIX + "/{ratingEngineId}/notes",
                shortenCustomerSpace(customerSpace), ratingEngineId);
        List list = get("Get all notes for single rating engine via rating engine id", url,
                List.class);
        return JsonUtils.convertList(list, RatingEngineNote.class);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Map<String, List<String>> getRatingEngineDependencies(String customerSpace,
            String ratingEngineId) {
        String url = constructUrl(URL_PREFIX + "/{ratingEngineId}/dependencies",
                shortenCustomerSpace(customerSpace), ratingEngineId);
        Map raw = get("Get all dependencies of the rating engine ", url, Map.class);

        return JsonUtils.convertMapWithListValue(raw, String.class, String.class);
    }

    public Boolean createNote(String customerSpace, String ratingEngineId, NoteParams noteParams) {
        String url = constructUrl(URL_PREFIX + "/{ratingEngineId}/notes",
                shortenCustomerSpace(customerSpace), ratingEngineId);
        return post("Insert one note for a certain rating engine.", url, noteParams, Boolean.class);
    }

    public void deleteNote(String customerSpace, String ratingEngineId, String noteId) {
        String url = constructUrl(URL_PREFIX + "/{ratingEngineId}/notes/{noteId}",
                shortenCustomerSpace(customerSpace), ratingEngineId, noteId);
        delete("Delete a note from a certain rating engine via rating engine id and note id.", url);
    }

    public Boolean updateNote(String customerSpace, String ratingEngineId, String noteId,
            NoteParams noteParams) {
        String url = constructUrl(URL_PREFIX + "/{ratingEngineId}/notes/{noteId}",
                shortenCustomerSpace(customerSpace), ratingEngineId, noteId);
        return post("Update the content of a certain note via note id.", url, noteParams,
                Boolean.class);
    }

    public EventFrontEndQuery getModelingQueryByRatingId(String customerSpace,
            String ratingEngineId, String aiModelId, ModelingQueryType modelingQueryType) {
        String url = constructUrl(
                URL_PREFIX + "/{ratingEngineId}/ratingmodels/{aiModel}/modelingquery?querytype="
                        + modelingQueryType,
                shortenCustomerSpace(customerSpace), ratingEngineId, aiModelId);
        return get("getModelingQuery", url, EventFrontEndQuery.class);
    }

    public EventFrontEndQuery getModelingQueryByRating(String customerSpace, String ratingEngineId,
            String aiModelId, ModelingQueryType modelingQueryType, RatingEngine ratingEngine) {
        String url = constructUrl(
                URL_PREFIX + "/{ratingEngineId}/ratingmodels/{aiModel}/modelingquery?querytype="
                        + modelingQueryType,
                shortenCustomerSpace(customerSpace), ratingEngineId, aiModelId);
        return post("getModelingQuery", url, ratingEngine, EventFrontEndQuery.class);
    }

    public Long getModelingQueryCountByRatingId(String customerSpace, String ratingEngineId,
            String aiModelId, ModelingQueryType modelingQueryType) {
        return getModelingQueryCountByRatingId(customerSpace, ratingEngineId, aiModelId,
                modelingQueryType, null);
    }

    public Long getModelingQueryCountByRatingId(String customerSpace, String ratingEngineId,
            String aiModelId, ModelingQueryType modelingQueryType, DataCollection.Version version) {
        String url = constructUrl(
                URL_PREFIX
                        + "/{ratingEngineId}/ratingmodels/{aiModel}/modelingquery/count?querytype="
                        + modelingQueryType,
                shortenCustomerSpace(customerSpace), ratingEngineId, aiModelId);
        if (version != null) {
            url += "&version=" + version;
        }
        return get("getModelingQueryCount", url, Long.class);
    }

    public Long getModelingQueryCountByRating(String customerSpace, String ratingEngineId,
            String aiModelId, ModelingQueryType modelingQueryType, RatingEngine ratingEngine) {
        return getModelingQueryCountByRating(customerSpace, ratingEngineId, aiModelId,
                modelingQueryType, ratingEngine, null);
    }

    public Long getModelingQueryCountByRating(String customerSpace, String ratingEngineId,
            String aiModelId, ModelingQueryType modelingQueryType, RatingEngine ratingEngine,
            DataCollection.Version version) {
        String url = constructUrl(
                URL_PREFIX + "/{ratingEngineId}/ratingmodels/{aiModel}/modelingquery/count"
                        + "?querytype=" + modelingQueryType,
                shortenCustomerSpace(customerSpace), ratingEngineId, aiModelId);
        if (version != null) {
            url += "&version=" + version;
        }
        return post("getModelingQueryCount", url, ratingEngine, Long.class);
    }

    public String modelRatingEngine(String customerSpace, String ratingEngineId,
            String ratingModelId, List<ColumnMetadata> attributes, String userEmail) {
        String url = constructUrl(URL_PREFIX
                + "/{ratingEngineId}/ratingmodels/{ratingModelId}/model?useremail={userEmail}",
                shortenCustomerSpace(customerSpace), ratingEngineId, ratingModelId, userEmail);
        return post("modelRatingEngine", url, attributes, String.class);
    }

    public boolean validateForModelingByRatingEngineId(String customerSpace, String ratingEngineId,
            String ratingModelId) {
        String url = constructUrl(
                URL_PREFIX + "/{ratingEngineId}/ratingmodels/{ratingModelId}/model/validate",
                shortenCustomerSpace(customerSpace), ratingEngineId, ratingModelId);
        return get("validateForModeling", url, Boolean.class);
    }

    public boolean validateForModeling(String customerSpace, String ratingEngineId,
            String ratingModelId, RatingEngine ratingEngine) {
        String url = constructUrl(
                URL_PREFIX + "/{ratingEngineId}/ratingmodels/{ratingModelId}/model/validate",
                shortenCustomerSpace(customerSpace), ratingEngineId, ratingModelId);
        return post("validateForModeling", url, ratingEngine, Boolean.class);
    }

    @RequestMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/setModelingStatus", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Get total count of Account and Contact as related to Rating Engine given its id")
    public void updateModelingStatus(@PathVariable String customerSpace, //
            @PathVariable String ratingEngineId, //
            @PathVariable String ratingModelId, //
            @RequestParam(value = "newStatus", required = true) JobStatus newStatus) {
        String url = constructUrl(URL_PREFIX
                + "/{ratingEngineId}/ratingmodels/{ratingModelId}/setModelingStatus?newStatus={newStatus}",
                shortenCustomerSpace(customerSpace), ratingEngineId, ratingModelId, newStatus);
        post("updateModelingStatus", url, null, Object.class);
    }

    public List<AttributeLookup> getDependingAttrsForModel(String customerSpace,
            RatingEngineType engineType, String modelId) {
        String url = constructUrl(URL_PREFIX + "/dependingattrs/type/{engineType}/model/{modelId}",
                shortenCustomerSpace(customerSpace), engineType, modelId);
        return getList("get depending attrs for rating model", url, AttributeLookup.class);
    }

    public List<AttributeLookup> getDependentAttrs(String customerSpace) {
        String url = constructUrl(URL_PREFIX + "/dependentattrs",
                shortenCustomerSpace(customerSpace));
        return getList("get dependent attrs for rating model", url, AttributeLookup.class);
    }

    @VisibleForTesting
    String constructUpdateRatingModelUrl(String customerSpace, String ratingEngineId,
            String ratingModelId, String user) {
        StringBuilder sb = new StringBuilder();
        sb.append(URL_PREFIX).append("/{ratingEngineId}/ratingmodels/{ratingModelId}");
        if (user != null) {
            sb.append("?user={user}");
        }
        return constructUrl(sb.toString(), shortenCustomerSpace(customerSpace), ratingEngineId,
                ratingModelId, user);
    }

    @VisibleForTesting
    String constructCreateOrUpdateRatingEngineUrl(String customerSpace, String user,
            Boolean unlinkSegment, Boolean creaateAction) {
        StringBuilder sb = new StringBuilder();
        sb.append(URL_PREFIX);
        List<Object> paramObjects = new ArrayList<>();
        paramObjects.add(shortenCustomerSpace(customerSpace));
        if (user != null) {
            sb.append("?user={user}");
            paramObjects.add(user);
        }
        if (unlinkSegment != null) {
            if (paramObjects.size() == 1) {
                sb.append("?");
            } else {
                sb.append("&");
            }
            sb.append("unlink-segment={unlink-segment}");
            paramObjects.add(unlinkSegment);
        }
        if (creaateAction != null) {
            if (paramObjects.size() == 1) {
                sb.append("?");
            } else {
                sb.append("&");
            }
            sb.append("create-action={create-action}");
            paramObjects.add(creaateAction);
        }

        return constructUrl(sb.toString(), paramObjects.toArray(new Object[paramObjects.size()]));
    }

    public DataPage getEntityPreview(String customerSpace, String ratingEngineId, long offset,
            long maximum, BusinessEntity entityType, String sortBy, Boolean descending,
            String bucketFieldName, List<String> lookupFieldNames,
            Boolean restrictNotNullSalesforceId, String freeFormTextSearch,
            List<String> selectedBuckets, String lookupIdColumn) {
        StringBuilder sb = new StringBuilder();
        sb.append(URL_PREFIX);
        sb.append(
                "/{ratingEngineId}/entitypreview?offset={offset}&maximum={maximum}&entityType={entityType}");
        List<Object> paramObjects = new ArrayList<>(Arrays.asList(
                shortenCustomerSpace(customerSpace), ratingEngineId, offset, maximum, entityType));

        addParam("sortBy", sortBy, sb, paramObjects);
        addParam("bucketFieldName", bucketFieldName, sb, paramObjects);
        addParam("descending", descending, sb, paramObjects);
        addParam("lookupFieldNames", lookupFieldNames, sb, paramObjects);
        addParam("restrictNotNullSalesforceId", restrictNotNullSalesforceId, sb, paramObjects);
        addParam("freeFormTextSearch", freeFormTextSearch, sb, paramObjects);
        addParam("selectedBuckets", selectedBuckets, sb, paramObjects);
        addParam("lookupIdColumn", lookupIdColumn, sb, paramObjects);

        String url = constructUrl(sb.toString(),
                paramObjects.toArray(new Object[paramObjects.size()]));
        return get("getEntityPreview", url, DataPage.class);
    }

    public Long getEntityPreviewCount(String customerSpace, String ratingEngineId,
            BusinessEntity entityType, Boolean restrictNotNullSalesforceId,
            String freeFormTextSearch, List<String> selectedBuckets, String lookupIdColumn) {
        StringBuilder sb = new StringBuilder();
        sb.append(URL_PREFIX);
        sb.append("/{ratingEngineId}/entitypreview/count?entityType={entityType}");
        List<Object> paramObjects = new ArrayList<>();
        paramObjects.addAll(
                Arrays.asList(shortenCustomerSpace(customerSpace), ratingEngineId, entityType));

        addParam("restrictNotNullSalesforceId", restrictNotNullSalesforceId, sb, paramObjects);
        addParam("freeFormTextSearch", freeFormTextSearch, sb, paramObjects);
        addParam("selectedBuckets", selectedBuckets, sb, paramObjects);
        addParam("lookupIdColumn", lookupIdColumn, sb, paramObjects);

        String url = constructUrl(sb.toString(),
                paramObjects.toArray(new Object[paramObjects.size()]));
        return get("getEntityPreview", url, Long.class);
    }

    private void addParam(String paramName, Object value, StringBuilder sb,
            List<Object> paramObjects) {
        if (value != null) {
            if (value instanceof String && StringUtils.isNotBlank((String) value)) {
                sb.append(String.format(URL_PARAM_TEMPLATE, paramName, paramName));
                paramObjects.add(((String) value).trim());
            } else if (value instanceof Boolean) {
                sb.append(String.format(URL_PARAM_TEMPLATE, paramName, paramName));
                paramObjects.add(value);
            } else if (value instanceof List && CollectionUtils.isNotEmpty((List<?>) value)) {
                ((List<?>) value).stream().forEach(v -> {
                    sb.append(String.format(URL_PARAM_TEMPLATE, paramName, paramName));
                    if (v instanceof String) {
                        paramObjects.add(((String) v).trim());
                    } else {
                        paramObjects.add(v);
                    }
                });
            }
        }
    }
}
