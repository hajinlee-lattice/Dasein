package com.latticeengines.apps.cdl.controller;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.annotation.Action;
import com.latticeengines.apps.cdl.service.RatingEngineDashboardService;
import com.latticeengines.apps.cdl.service.RatingEngineNoteService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.service.RatingEntityPreviewService;
import com.latticeengines.apps.cdl.util.ActionContext;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.ActionConfiguration;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineDashboard;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingModelWithPublishedHistoryDTO;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CustomEventModelingConfig;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.workflow.JobStatus;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "ratingengine", description = "REST resource for rating engine")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/ratingengines")
public class RatingEngineResource {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineResource.class);

    private final RatingEngineService ratingEngineService;

    private final RatingEngineNoteService ratingEngineNoteService;

    private final RatingEngineDashboardService ratingEngineDashboardService;

    private final RatingEntityPreviewService ratingEntityPreviewService;

    private final ActionService actionService;

    @Inject
    public RatingEngineResource(RatingEngineService ratingEngineService, //
            RatingEngineNoteService ratingEngineNoteService, //
            RatingEngineDashboardService ratingEngineDashboardService, //
            RatingEntityPreviewService ratingEntityPreviewService, //
            ActionService actionService) {
        this.ratingEngineService = ratingEngineService;
        this.ratingEngineNoteService = ratingEngineNoteService;
        this.ratingEngineDashboardService = ratingEngineDashboardService;
        this.ratingEntityPreviewService = ratingEntityPreviewService;
        this.actionService = actionService;
    }

    // -------------
    // RatingEngines
    // -------------
    @GetMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "Get all Rating Engine summaries for a tenant")
    public List<RatingEngineSummary> getRatingEngineSummaries( //
            @PathVariable String customerSpace,
            @RequestParam(value = "status", required = false) String status, //
            @RequestParam(value = "type", required = false) String type,
            @RequestParam(value = "publishedratingsonly", required = false, defaultValue = "false") Boolean publishedRatingsOnly) {
        return ratingEngineService.getAllRatingEngineSummaries(type, status, publishedRatingsOnly);
    }

    @GetMapping(value = "/deleted")
    @ResponseBody
    @ApiOperation(value = "Get all Deleted Rating Engines")
    public List<RatingEngine> getAllDeletedRatingEngines(@PathVariable String customerSpace) {
        return ratingEngineService.getAllDeletedRatingEngines();
    }

    @GetMapping(value = "/types")
    @ResponseBody
    @ApiOperation(value = "Get types for Rating Engines")
    public List<RatingEngineType> getRatingEngineTypes(@PathVariable String customerSpace) {
        return Arrays.asList(RatingEngineType.values());
    }

    @GetMapping(value = "/ids")
    @ResponseBody
    @ApiOperation(value = "Get ids for published Rating Engines. Can be filtered by segment name.")
    public List<String> getRatingEngineIds(@PathVariable String customerSpace,
            @RequestParam(value = "segment", required = false) String segment) {
        return ratingEngineService.getAllRatingEngineIdsInSegment(segment);
    }

    @GetMapping(value = "/{ratingEngineId}")
    @ResponseBody
    @ApiOperation(value = "Get a Rating Engine given its id")
    public RatingEngine getRatingEngine(@PathVariable String customerSpace,
            @PathVariable String ratingEngineId) {
        return ratingEngineService.getRatingEngineById(ratingEngineId, true, true);
    }

    @PostMapping(value = "")
    @ResponseBody
    @Action
    @ApiOperation(value = "Register or update a Rating Engine")
    public RatingEngine createOrUpdateRatingEngine(@PathVariable String customerSpace,
            @RequestBody RatingEngine ratingEngine,
            @RequestParam(value = "user", required = false, defaultValue = "DEFAULT_USER") String user,
            @RequestParam(value = "unlink-segment", required = false, defaultValue = "false") Boolean unlinkSegment,
            @RequestParam(value = "create-action", required = false, defaultValue = "true") Boolean createAction) {
        if (StringUtils.isEmpty(customerSpace)) {
            throw new LedpException(LedpCode.LEDP_39002);
        }
        if (ratingEngine == null) {
            throw new NullPointerException("Rating Engine is null.");
        }
        RatingEngine res = ratingEngineService.createOrUpdate(ratingEngine, unlinkSegment);
        if (createAction) {
            registerAction(ActionContext.getAction(), user);
        }
        return res;
    }

    private void registerAction(com.latticeengines.domain.exposed.pls.Action action, String user) {
        if (action != null) {
            action.setTenant(MultiTenantContext.getTenant());
            action.setActionInitiator(user);
            log.info(String.format("Registering action %s", action));
            ActionConfiguration actionConfig = action.getActionConfiguration();
            if (actionConfig != null) {
                action.setDescription(actionConfig.serialize());
            }
            actionService.create(action);
        }
    }

    @PostMapping(value = "/replicate/{engineId}")
    @ResponseBody
    @ApiOperation(value = "Replicate a Rating Engine")
    public RatingEngine replicateRatingEngine(@PathVariable String customerSpace,
            @PathVariable String engineId) {
        return ratingEngineService.replicateRatingEngine(engineId);
    }

    @DeleteMapping(value = "/{ratingEngineId}")
    @ResponseBody
    @ApiOperation(value = "Delete a Rating Engine given its id")
    public Boolean deleteRatingEngine(@PathVariable String customerSpace,
            @PathVariable String ratingEngineId, //
            @RequestParam(value = "hard-delete", required = false, defaultValue = "false") Boolean hardDelete, //
            @RequestParam(value = "action-initiator", required = false) String actionInitiator) {
        log.info(String.format("Delete rating engine %s, action initiated by %s ", ratingEngineId,
                actionInitiator));
        ratingEngineService.deleteById(ratingEngineId, hardDelete, actionInitiator);
        return true;
    }

    @PutMapping(value = "/{ratingEngineId}/revertdelete")
    @ResponseBody
    @ApiOperation(value = "Delete a Rating Engine given its id")
    public Boolean revertDeleteRatingEngine(@PathVariable String customerSpace,
            @PathVariable String ratingEngineId) {
        ratingEngineService.revertDelete(ratingEngineId);
        return true;
    }

    @PutMapping(value = "/{ratingEngineId}/counts")
    @ResponseBody
    @ApiOperation(value = "Update rating engine counts")
    public Map<String, Long> updateRatingEngineCounts(@PathVariable String customerSpace,
            @PathVariable String ratingEngineId) {
        return ratingEngineService.updateRatingEngineCounts(ratingEngineId);
    }

    @GetMapping(value = "/{ratingEngineId}/dependencies")
    @ResponseBody
    @ApiOperation(value = "Get all the dependencies for single rating engine via rating engine id.")
    public Map<String, List<String>> getRatingEngineDependencies(@PathVariable String customerSpace,
            @PathVariable String ratingEngineId) {
        log.info(String.format("get all ratingEngineNotes by ratingEngineId=%s", ratingEngineId));
        return ratingEngineService.getRatingEngineDependencies(customerSpace, ratingEngineId);
    }

    @GetMapping(value = "/{ratingEngineId}/dashboard", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get dashboard info for Rating Engine given its id")
    public RatingEngineDashboard getRatingEngineDashboardById(@PathVariable String customerSpace,
            @PathVariable String ratingEngineId) {
        return ratingEngineDashboardService
                .getRatingsDashboard(CustomerSpace.parse(customerSpace).toString(), ratingEngineId);
    }

    @GetMapping(value = "/{ratingEngineId}/publishedhistory", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a published bucket metadata per iteration of a rating engine given its id")
    public List<RatingModelWithPublishedHistoryDTO> getPublishedHistory(
            @PathVariable String customerSpace, @PathVariable String ratingEngineId) {
        return ratingEngineService
                .getPublishedHistory(CustomerSpace.parse(customerSpace).toString(), ratingEngineId);
    }

    @GetMapping(value = "/{ratingEngineId}/entitypreview", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get preview of Account and Contact as related to Rating Engine given its id")
    public DataPage getEntityPreview( //
            @PathVariable String customerSpace, //
            @PathVariable String ratingEngineId, //
            @RequestParam(value = "offset", required = true) long offset, //
            @RequestParam(value = "maximum", required = true) long maximum, //
            @RequestParam(value = "entityType", required = true) BusinessEntity entityType, //
            @RequestParam(value = "sortBy", required = false) String sortBy, //
            @RequestParam(value = "bucketFieldName", required = false) String bucketFieldName, //
            @RequestParam(value = "descending", required = false) Boolean descending, //
            @RequestParam(value = "lookupFieldNames", required = false) List<String> lookupFieldNames, //
            @RequestParam(value = "restrictNotNullSalesforceId", required = false) Boolean restrictNotNullSalesforceId, //
            @RequestParam(value = "freeFormTextSearch", required = false) String freeFormTextSearch, //
            @RequestParam(value = "selectedBuckets", required = false) List<String> selectedBuckets, //
            @RequestParam(value = "lookupIdColumn", required = false) String lookupIdColumn) {
        RatingEngine ratingEngine = ratingEngineService.getRatingEngineById(ratingEngineId, false,
                false);

        descending = descending == null ? false : descending;
        if (StringUtils.isNotBlank(freeFormTextSearch)) {
            try {
                freeFormTextSearch = URLDecoder.decode(freeFormTextSearch, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                log.warn("Failed to decode free form text search " + freeFormTextSearch, e);
            }
        }

        return ratingEntityPreviewService.getEntityPreview(ratingEngine, offset, maximum,
                entityType, sortBy, descending, bucketFieldName, lookupFieldNames,
                restrictNotNullSalesforceId, freeFormTextSearch, selectedBuckets, lookupIdColumn);
    }

    @GetMapping(value = "/{ratingEngineId}/entitypreview/count", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get total count of Account and Contact as related to Rating Engine given its id")
    public Long getEntityPreviewCount( //
            @PathVariable String customerSpace, //
            @PathVariable String ratingEngineId, //
            @RequestParam(value = "entityType", required = true) BusinessEntity entityType, //
            @RequestParam(value = "restrictNotNullSalesforceId", required = false) Boolean restrictNotNullSalesforceId, //
            @RequestParam(value = "freeFormTextSearch", required = false) String freeFormTextSearch, //
            @RequestParam(value = "selectedBuckets", required = false) List<String> selectedBuckets, //
            @RequestParam(value = "lookupIdColumn", required = false) String lookupIdColumn) {
        RatingEngine ratingEngine = ratingEngineService.getRatingEngineById(ratingEngineId, false,
                false);

        return ratingEntityPreviewService.getEntityPreviewCount(ratingEngine, entityType,
                restrictNotNullSalesforceId, freeFormTextSearch, selectedBuckets, lookupIdColumn);
    }
    // -------------
    // RatingEngines
    // -------------

    // -------------
    // RatingModels
    // -------------
    @GetMapping(value = "/{ratingEngineId}/ratingmodels", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get Rating Models associated with a Rating Engine given its id")
    public List<RatingModel> getRatingModels(@PathVariable String customerSpace,
            @PathVariable String ratingEngineId) {
        return ratingEngineService.getRatingModelsByRatingEngineId(ratingEngineId);
    }

    @PostMapping(value = "/{ratingEngineId}/ratingmodels", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create an iteration for a particular Rating Model associated with a Rating Engine given its Rating Engine id and Rating Model id")
    public RatingModel createModelIteration(@PathVariable String customerSpace,
            @PathVariable String ratingEngineId, @RequestBody RatingModel ratingModel) {
        RatingEngine ratingEngine = getRatingEngine(customerSpace, ratingEngineId);
        return ratingEngineService.createModelIteration(ratingEngine, ratingModel);
    }

    @GetMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a particular Rating Model associated with a Rating Engine given its Rating Engine id and Rating Model id")
    public RatingModel getRatingModel(@PathVariable String customerSpace,
            @PathVariable String ratingEngineId, @PathVariable String ratingModelId) {
        return ratingEngineService.getRatingModel(ratingEngineId, ratingModelId);
    }

    @PostMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}", headers = "Accept=application/json")
    @ResponseBody
    @Action
    @ApiOperation(value = "Update a particular Rating Model associated with a Rating Engine given its Rating Engine id and Rating Model id")
    public RatingModel updateRatingModel(@PathVariable String customerSpace, //
            @RequestBody RatingModel ratingModel, //
            @PathVariable String ratingEngineId, //
            @PathVariable String ratingModelId, //
            @RequestParam(value = "user", required = false, defaultValue = "DEFAULT_USER") String user) {
        RatingModel updatedRatingModel = ratingEngineService.updateRatingModel(ratingEngineId,
                ratingModelId, ratingModel);
        registerAction(ActionContext.getAction(), user);
        return updatedRatingModel;
    }

    @GetMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/attributes", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get Metadata for a given AIModel's iteration and data stores")
    @Deprecated
    public Map<String, List<ColumnMetadata>> getIterationAttributes(
            @PathVariable String customerSpace, @PathVariable String ratingEngineId,
            @PathVariable String ratingModelId, //
            @RequestParam(value = "data_stores", defaultValue = "", required = false) String dataStores) {
        List<CustomEventModelingConfig.DataStore> stores = null;
        if (StringUtils.isNotEmpty(dataStores)) {
            stores = Arrays.asList(dataStores.split(",")).stream().map(x -> {
                if (EnumUtils.isValidEnum(CustomEventModelingConfig.DataStore.class, x)) {
                    return CustomEventModelingConfig.DataStore.valueOf(x);
                } else {
                    throw new LedpException(LedpCode.LEDP_32000,
                            new String[] { "Invalid DataStore " + x });
                }
            }).collect(Collectors.toList());
        }
        return ratingEngineService.getIterationAttributes(customerSpace, ratingEngineId,
                ratingModelId, stores);

    }

    @GetMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/metadata", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get Metadata for a given AIModel's iteration and data stores")
    public List<ColumnMetadata> getIterationMetadata(@PathVariable String customerSpace,
            @PathVariable String ratingEngineId, @PathVariable String ratingModelId, //
            @RequestParam(value = "data_stores", defaultValue = "", required = false) String dataStores) {
        List<CustomEventModelingConfig.DataStore> stores = null;
        if (StringUtils.isNotEmpty(dataStores)) {
            stores = Arrays.asList(dataStores.split(",")).stream().map(x -> {
                if (EnumUtils.isValidEnum(CustomEventModelingConfig.DataStore.class, x)) {
                    return CustomEventModelingConfig.DataStore.valueOf(x);
                } else {
                    throw new LedpException(LedpCode.LEDP_32000,
                            new String[] { "Invalid DataStore " + x });
                }
            }).collect(Collectors.toList());
        }
        return ratingEngineService.getIterationMetadata(customerSpace, ratingEngineId,
                ratingModelId, stores);

    }

    @GetMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/metadata/cube", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get Metadata for a given AIModel's iteration and data stores")
    public Map<String, StatsCube> getIterationMetadataCube(@PathVariable String customerSpace,
            @PathVariable String ratingEngineId, @PathVariable String ratingModelId, //
            @RequestParam(value = "data_stores", defaultValue = "", required = false) String dataStores) {
        List<CustomEventModelingConfig.DataStore> stores = null;
        if (StringUtils.isNotEmpty(dataStores)) {
            stores = Arrays.asList(dataStores.split(",")).stream().map(x -> {
                if (EnumUtils.isValidEnum(CustomEventModelingConfig.DataStore.class, x)) {
                    return CustomEventModelingConfig.DataStore.valueOf(x);
                } else {
                    throw new LedpException(LedpCode.LEDP_32000,
                            new String[] { "Invalid DataStore " + x });
                }
            }).collect(Collectors.toList());
        }
        return ratingEngineService.getIterationMetadataCube(customerSpace, ratingEngineId,
                ratingModelId, stores);

    }

    @GetMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/metadata/topn", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get Metadata for a given AIModel's iteration and data stores")
    public TopNTree getIterationMetadataTopN(@PathVariable String customerSpace,
            @PathVariable String ratingEngineId, @PathVariable String ratingModelId, //
            @RequestParam(value = "data_stores", defaultValue = "", required = false) String dataStores) {
        List<CustomEventModelingConfig.DataStore> stores = null;
        if (StringUtils.isNotEmpty(dataStores)) {
            stores = Arrays.asList(dataStores.split(",")).stream().map(x -> {
                if (EnumUtils.isValidEnum(CustomEventModelingConfig.DataStore.class, x)) {
                    return CustomEventModelingConfig.DataStore.valueOf(x);
                } else {
                    throw new LedpException(LedpCode.LEDP_32000,
                            new String[] { "Invalid DataStore " + x });
                }
            }).collect(Collectors.toList());
        }
        return ratingEngineService.getIterationMetadataTopN(customerSpace, ratingEngineId,
                ratingModelId, stores);

    }

    @GetMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/modelingquery")
    @ResponseBody
    @ApiOperation(value = "Return a EventFrontEndQuery corresponding to the given rating engine, rating model and modelingquerytype")
    public EventFrontEndQuery getModelingQueryByRatingId(@PathVariable String customerSpace, //
            @PathVariable String ratingEngineId, @PathVariable String ratingModelId, //
            @RequestParam(value = "querytype") ModelingQueryType modelingQueryType, //
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        RatingEngine ratingEngine = getRatingEngine(customerSpace, ratingEngineId);
        return getModelingQueryByRating(customerSpace, ratingEngineId, ratingModelId,
                modelingQueryType, version, ratingEngine);
    }

    @PostMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/modelingquery")
    @ResponseBody
    @ApiOperation(value = "Return a EventFrontEndQuery corresponding to the given rating engine, rating model and modelingquerytype")
    public EventFrontEndQuery getModelingQueryByRating(@PathVariable String customerSpace, //
            @PathVariable String ratingEngineId, @PathVariable String ratingModelId, //
            @RequestParam(value = "querytype") ModelingQueryType modelingQueryType, //
            @RequestParam(value = "version", required = false) DataCollection.Version version, //
            @RequestBody RatingEngine ratingEngine) {
        RatingModel ratingModel;
        if (ratingEngine == null) {
            throw new LedpException(LedpCode.LEDP_40013);
        } else {
            ratingModel = ratingEngine.getLatestIteration();
        }
        return ratingEngineService.getModelingQuery(customerSpace, ratingEngine, ratingModel,
                modelingQueryType, version);
    }

    @GetMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/modelingquery/count")
    @ResponseBody
    @ApiOperation(value = "Return a EventFrontEndQuery corresponding to the given rating engine, rating model and modelingquerytype")
    public Long getModelingQueryCountByRatingId(@PathVariable String customerSpace, //
            @PathVariable String ratingEngineId, //
            @PathVariable String ratingModelId, //
            @RequestParam(value = "querytype", required = true) ModelingQueryType modelingQueryType, //
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        RatingEngine ratingEngine = getRatingEngine(customerSpace, ratingEngineId);
        return getModelingQueryCountByRatingEngine(customerSpace, ratingEngineId, ratingModelId,
                modelingQueryType, version, ratingEngine);
    }

    @PostMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/modelingquery/count")
    @ResponseBody
    @ApiOperation(value = "Return a EventFrontEndQuery corresponding to the given rating engine, rating model and modelingquerytype")
    public Long getModelingQueryCountByRatingEngine(@PathVariable String customerSpace, //
            @PathVariable String ratingEngineId, //
            @PathVariable String ratingModelId, //
            @RequestParam(value = "querytype") ModelingQueryType modelingQueryType,

            @RequestParam(value = "version", required = false) DataCollection.Version version,
            @RequestBody RatingEngine ratingEngine) {
        RatingModel ratingModel;
        if (ratingEngine == null) {
            throw new LedpException(LedpCode.LEDP_40013);
        } else {
            ratingModel = ratingEngine.getLatestIteration();
        }
        return ratingEngineService.getModelingQueryCount(customerSpace, ratingEngine, ratingModel,
                modelingQueryType, version);

    }

    @GetMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/model/validate")
    @ResponseBody
    @ApiOperation(value = "Validate whether the given RatingModel of the Rating Engine is valid for modeling")
    public boolean validateForModelingByRatingId(@PathVariable String customerSpace, //
            @PathVariable String ratingEngineId, //
            @PathVariable String ratingModelId) {
        RatingEngine ratingEngine = getRatingEngine(customerSpace, ratingEngineId);
        RatingModel ratingModel = getRatingModel(customerSpace, ratingEngineId, ratingModelId);

        if (!(ratingModel instanceof AIModel)) {
            throw new LedpException(LedpCode.LEDP_31107,
                    new String[] { ratingModel.getClass().getName() });
        }

        return ratingEngineService.validateForModeling(customerSpace, ratingEngine,
                (AIModel) ratingModel);
    }

    @PostMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/model/validate")
    @ResponseBody
    @ApiOperation(value = "Validate whether the given RatingModel of the Rating Engine is valid for modeling")
    public boolean validateForModeling(@PathVariable String customerSpace, //
            @PathVariable String ratingEngineId, //
            @PathVariable String ratingModelId, //
            @RequestBody RatingEngine ratingEngine) {
        RatingModel ratingModel;
        if (ratingEngine == null) {
            throw new LedpException(LedpCode.LEDP_40013);
        }

        ratingModel = ratingEngine.getLatestIteration();
        if (ratingModel == null || !(ratingModel instanceof AIModel)) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] {
                    "LatestIteration of the given Model is Null or unsupported for validation" });
        }

        return ratingEngineService.validateForModeling(customerSpace, ratingEngine,
                (AIModel) ratingModel);
    }

    @PostMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/model")
    @ResponseBody
    @ApiOperation(value = "Kick off modeling job for a Rating Engine AI model and return the job id. Returns the job id if the modeling job already exists.")
    public String modelRatingEngine(@PathVariable String customerSpace, //
            @PathVariable String ratingEngineId, //
            @PathVariable String ratingModelId, //
            @RequestBody(required = false) List<ColumnMetadata> attributes, //
            @RequestParam(value = "useremail", required = true) String userEmail) {
        RatingEngine ratingEngine = getRatingEngine(customerSpace, ratingEngineId);
        RatingModel ratingModel = getRatingModel(customerSpace, ratingEngineId, ratingModelId);

        if (!(ratingModel instanceof AIModel)) {
            throw new LedpException(LedpCode.LEDP_31107,
                    new String[] { ratingModel.getClass().getName() });
        }

        return ratingEngineService.modelRatingEngine(customerSpace, ratingEngine,
                (AIModel) ratingModel, attributes, userEmail);
    }

    @PostMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/setModelingStatus")
    @ResponseBody
    @ApiOperation(value = "Get total count of Account and Contact as related to Rating Engine given its id")
    public void updateModelingStatus(@PathVariable String customerSpace, //
            @PathVariable String ratingEngineId, //
            @PathVariable String ratingModelId, //
            @RequestParam(value = "newStatus", required = true) JobStatus newStatus) {
        ratingEngineService.updateModelingJobStatus(ratingEngineId, ratingModelId, newStatus);
    }

    @PostMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/setScoringIteration")
    @ResponseBody
    @ApiOperation(value = "Get total count of Account and Contact as related to Rating Engine given its id")
    public void setScoringIteration(@PathVariable String customerSpace, //
            @PathVariable String ratingEngineId, //
            @PathVariable String ratingModelId, //
            @RequestBody(required = false) List<BucketMetadata> bucketMetadatas, //
            @RequestParam(value = "useremail", required = true) String userEmail) {
        ratingEngineService.setScoringIteration(customerSpace, ratingEngineId, ratingModelId, bucketMetadatas,
                userEmail);
    }
    // -------------
    // RatingModels
    // -------------

    // ------------------
    // RatingEngine Notes
    // ------------------
    @GetMapping(value = "/{ratingEngineId}/notes")
    @ResponseBody
    @ApiOperation(value = "Get all notes for single rating engine via rating engine id.")
    public List<RatingEngineNote> getAllNotes(@PathVariable String customerSpace,
            @PathVariable String ratingEngineId) {
        log.info(String.format("get all ratingEngineNotes by ratingEngineId=%s", ratingEngineId));
        return ratingEngineNoteService.getAllByRatingEngineId(ratingEngineId);
    }

    @PostMapping(value = "/{ratingEngineId}/notes")
    @ResponseBody
    @ApiOperation(value = "Insert one note for a certain rating engine.")
    public Boolean createNote(@PathVariable String customerSpace,
            @PathVariable String ratingEngineId, @RequestBody NoteParams noteParams) {
        log.info(String.format("RatingEngineId=%s's note createdUser=%s", ratingEngineId,
                noteParams.getUserName()));
        ratingEngineNoteService.create(ratingEngineId, noteParams);
        return Boolean.TRUE;
    }

    @DeleteMapping(value = "/{ratingEngineId}/notes/{noteId}")
    @ResponseBody
    @ApiOperation(value = "Delete a note from a certain rating engine via rating engine id and note id.")
    public void deleteNote(@PathVariable String customerSpace, @PathVariable String ratingEngineId,
            @PathVariable String noteId) {
        log.info(String.format("RatingEngineNoteId=%s deleted by user=%s", noteId,
                MultiTenantContext.getEmailAddress()));
        ratingEngineNoteService.deleteById(noteId);
    }

    @PostMapping(value = "/{ratingEngineId}/notes/{noteId}")
    @ResponseBody
    @ApiOperation(value = "Update the content of a certain note via note id.")
    public Boolean updateNote(@PathVariable String customerSpace,
            @PathVariable String ratingEngineId, @PathVariable String noteId,
            @RequestBody NoteParams noteParams) {
        log.info(String.format("RatingEngineNoteId=%s update by %s", noteId,
                noteParams.getUserName()));
        ratingEngineNoteService.updateById(noteId, noteParams);
        return Boolean.TRUE;
    }
    // ------------------
    // RatingEngine Notes
    // ------------------

    // -------------------
    // RatingEngine Others
    // -------------------
    @GetMapping(value = "/dependingattrs/type/{engineType}/model/{modelId}")
    @ResponseBody
    @ApiOperation(value = "Get dashboard info for Rating Engine given its id")
    public List<AttributeLookup> getDependingAttrsForModel(@PathVariable String customerSpace,
            @PathVariable RatingEngineType engineType, @PathVariable String modelId) {
        return ratingEngineService.getDependingAttrsInModel(engineType, modelId);
    }

    // -------------------
    // RatingEngine
    // get all models created in customer space
    // -------------------
    @GetMapping(value = "/allmodels")
    @ResponseBody
    @ApiOperation(value = "get all models in customer space")
    public List<RatingModel> getAllModels(@PathVariable String customerSpace) {
        return ratingEngineService.getAllRatingModels();
    }
}
