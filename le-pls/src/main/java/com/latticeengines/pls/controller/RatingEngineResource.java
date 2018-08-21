package com.latticeengines.pls.controller;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.RequestEntity;
import org.springframework.security.access.prepost.PreAuthorize;
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
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.json.MappingJackson2JsonView;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionConfiguration;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineAndActionDTO;
import com.latticeengines.domain.exposed.pls.RatingEngineDashboard;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingModelAndActionDTO;
import com.latticeengines.domain.exposed.pls.RatingModelWithPublishedHistoryDTO;
import com.latticeengines.domain.exposed.pls.frontend.Status;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;
import com.latticeengines.domain.exposed.pls.frontend.View;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.ratings.coverage.RatingsCountRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingsCountResponse;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.ActionService;
import com.latticeengines.pls.service.impl.GraphDependencyToUIActionUtil;
import com.latticeengines.proxy.exposed.cdl.RatingCoverageProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineDashboardProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "ratingengine", description = "REST resource for rating engine")
@RestController
@RequestMapping("/ratingengines")
@PreAuthorize("hasRole('View_PLS_RatingEngines')")
public class RatingEngineResource {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineResource.class);

    private static final String RATING_DELETE_FAILED_TITLE = "Cannot delete Model";
    private static final String RATING_DELETE_FAILED_MODEL_IN_USE_TITLE = "Model In Use";
    private static final String RATING_DELETE_FAILED_MODEL_IN_USE = "This model is in use and cannot be deleted until the dependency has been removed.";

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private RatingEngineDashboardProxy ratingEngineDashboardProxy;

    @Inject
    private RatingCoverageProxy ratingCoverageProxy;

    @Inject
    private GraphDependencyToUIActionUtil graphDependencyToUIActionUtil;

    @Inject
    private ActionService actionService;

    @GetMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "Get all Rating Engine summaries for a tenant")
    public List<RatingEngineSummary> getRatingEngineSummaries( //
            @RequestParam(value = "status", required = false) String status, //
            @RequestParam(value = "type", required = false) String type, //
            @RequestParam(value = "publishedratingsonly", required = false, defaultValue = "false") Boolean publishedRatingsOnly) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getRatingEngineSummaries(tenant.getId(), status, type, publishedRatingsOnly);
    }

    @GetMapping(value = "/deleted")
    @ResponseBody
    @ApiOperation(value = "Get all Deleted Rating Engines")
    public List<RatingEngine> getAllDeletedRatingEngines() {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getAllDeletedRatingEngines(tenant.getId());
    }

    @GetMapping(value = "/types")
    @ResponseBody
    @ApiOperation(value = "Get types for Rating Engines")
    public List<RatingEngineType> getRatingEngineTypes() {
        return Arrays.asList(RatingEngineType.values());
    }

    @GetMapping(value = "/{ratingEngineId}")
    @ResponseBody
    @ApiOperation(value = "Get a Rating Engine given its id")
    public RatingEngine getRatingEngine( //
            @PathVariable String ratingEngineId, //
            HttpServletRequest request, //
            HttpServletResponse response) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getRatingEngine(tenant.getId(), ratingEngineId);
    }

    @GetMapping(value = "/{ratingEngineId}/entitypreview")
    @ResponseBody
    @ApiOperation(value = "Get preview of Account and Contact as related to Rating Engine given its id")
    public DataPage getEntityPreview(RequestEntity<String> requestEntity, //
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
        descending = descending == null ? false : descending;
        Tenant tenant = MultiTenantContext.getTenant();

        return ratingEngineProxy.getEntityPreview(tenant.getId(), ratingEngineId, offset, maximum, entityType, sortBy,
                descending, bucketFieldName, lookupFieldNames, restrictNotNullSalesforceId, freeFormTextSearch,
                selectedBuckets, lookupIdColumn);
    }

    @GetMapping(value = "/{ratingEngineId}/entitypreview/count")
    @ResponseBody
    @ApiOperation(value = "Get total count of Account and Contact as relatedto Rating Engine given its id")
    public Long getEntityPreviewCount(RequestEntity<String> requestEntity, //
            @PathVariable String ratingEngineId, //
            @RequestParam(value = "entityType", required = true) BusinessEntity entityType, //
            @RequestParam(value = "restrictNotNullSalesforceId", required = false) Boolean restrictNotNullSalesforceId, //
            @RequestParam(value = "freeFormTextSearch", required = false) String freeFormTextSearch, //
            @RequestParam(value = "selectedBuckets", required = false) List<String> selectedBuckets, //
            @RequestParam(value = "lookupIdColumn", required = false) String lookupIdColumn) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getEntityPreviewCount(tenant.getId(), ratingEngineId, entityType,
                restrictNotNullSalesforceId, freeFormTextSearch, selectedBuckets, lookupIdColumn);
    }

    @GetMapping(value = "/{ratingEngineId}/dashboard")
    @ResponseBody
    @ApiOperation(value = "Get dashboard info for Rating Engine given its id")
    public RatingEngineDashboard getRatingsDashboard(@PathVariable String ratingEngineId) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineDashboardProxy.getRatingEngineDashboardById(tenant.getId(), ratingEngineId);
    }

    @GetMapping(value = "/{ratingEngineId}/publishedhistory", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a published bucket metadata per iteration of a rating engine given its id")
    public List<RatingModelWithPublishedHistoryDTO> getPublishedHistory(@PathVariable String ratingEngineId) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getPublishedHistory(tenant.getId(), ratingEngineId);
    }

    @PostMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "Register or update a Rating Engine")
    @PreAuthorize("hasRole('Create_PLS_RatingEngines')")
    public RatingEngine createOrUpdateRatingEngine(@RequestBody RatingEngine ratingEngine,
            @RequestParam(value = "unlink-segment", required = false, defaultValue = "false") Boolean unlinkSegment) {
        Tenant tenant = MultiTenantContext.getTenant();
        RatingEngineAndActionDTO ratingEngineAndAction;
        try {
            ratingEngineAndAction = ratingEngineProxy.createOrUpdateRatingEngineAndActionDTO(tenant.getId(),
                    ratingEngine, unlinkSegment);
        } catch (Exception ex) {
            throw graphDependencyToUIActionUtil.handleExceptionForCreateOrUpdate(ex, LedpCode.LEDP_40041);
        }

        Action action = ratingEngineAndAction.getAction();
        registerAction(action, tenant);
        return ratingEngineAndAction.getRatingEngine();
    }

    @DeleteMapping(value = "/{ratingEngineId}")
    @ResponseBody
    @ApiOperation(value = "Delete a Rating Engine given its id")
    @PreAuthorize("hasRole('Edit_PLS_RatingEngines')")
    public Boolean deleteRatingEngine(@PathVariable String ratingEngineId, //
            @RequestParam(value = "hard-delete", required = false, defaultValue = "false") Boolean hardDelete) {
        Tenant tenant = MultiTenantContext.getTenant();
        try {
            ratingEngineProxy.deleteRatingEngine(tenant.getId(), ratingEngineId, hardDelete,
                    MultiTenantContext.getEmailAddress());
        } catch (Exception ex) {
            if (ex instanceof LedpException) {
                LedpException exp = (LedpException) ex;
                if (exp.getCode() == LedpCode.LEDP_40042) {
                    throw graphDependencyToUIActionUtil.handleExceptionForCreateOrUpdate(ex, LedpCode.LEDP_40042,
                            View.Modal, RATING_DELETE_FAILED_MODEL_IN_USE_TITLE, RATING_DELETE_FAILED_MODEL_IN_USE);
                } else if (exp.getCode() == LedpCode.LEDP_18181) {
                    throw graphDependencyToUIActionUtil.handleExceptionForCreateOrUpdate(ex, LedpCode.LEDP_18181,
                            View.Modal, RATING_DELETE_FAILED_TITLE, null);
                } else {
                    throw graphDependencyToUIActionUtil.handleExceptionForCreateOrUpdate(ex, exp.getCode(), View.Banner,
                            RATING_DELETE_FAILED_TITLE, null);
                }
            } else {
                throw graphDependencyToUIActionUtil.handleExceptionForCreateOrUpdate(ex, null, View.Banner,
                        RATING_DELETE_FAILED_TITLE, null);
            }
        }

        return true;
    }

    @PutMapping(value = "/{ratingEngineId}/revertdelete")
    @ResponseBody
    @ApiOperation(value = "Delete a Rating Engine given its id")
    public Boolean revertDeleteRatingEngine(@PathVariable String ratingEngineId) {
        Tenant tenant = MultiTenantContext.getTenant();
        ratingEngineProxy.revertDeleteRatingEngine(tenant.getId(), ratingEngineId);
        return true;
    }

    @PostMapping(value = "/coverage")
    @ResponseBody
    @ApiOperation(value = "Get CoverageInfo for ids in Rating count request")
    public RatingsCountResponse getRatingEngineCoverageInfo(@RequestBody RatingsCountRequest ratingModelSegmentIds) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingCoverageProxy.getCoverageInfo(tenant.getId(), ratingModelSegmentIds);
    }

    @GetMapping(value = "/{ratingEngineId}/ratingmodels")
    @ResponseBody
    @ApiOperation(value = "Get Rating Models associated with a Rating Engine given its id")
    public List<RatingModel> getRatingModels(@PathVariable String ratingEngineId) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getRatingModels(tenant.getId(), ratingEngineId);
    }

    @PostMapping(value = "/{ratingEngineId}/ratingmodels")
    @ResponseBody
    @ApiOperation(value = "Create a Rating Model iteration associated with a Rating Engine given its id")
    public RatingModel createModelIteration(@PathVariable String ratingEngineId, @RequestBody RatingModel ratingModel) {
        Tenant tenant = MultiTenantContext.getTenant();
        try {
            return ratingEngineProxy.createModelIteration(tenant.getId(), ratingEngineId, ratingModel);
        } catch (Exception ex) {
            throw graphDependencyToUIActionUtil.handleExceptionForCreateOrUpdate(ex, LedpCode.LEDP_40041);
        }
    }

    @GetMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}")
    @ResponseBody
    @ApiOperation(value = "Get a particular Rating Model associated with a Rating Engine given its Rating Engine id and Rating Model id")
    public RatingModel getRatingModel(@PathVariable String ratingEngineId, @PathVariable String ratingModelId) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getRatingModel(tenant.getId(), ratingEngineId, ratingModelId);
    }

    @PostMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}")
    @ResponseBody
    @ApiOperation(value = "Update a particular Rating Model associated with a Rating Engine given its Rating Engine id and Rating Model id")
    public RatingModel updateRatingModel(@RequestBody RatingModel ratingModel, @PathVariable String ratingEngineId,
            @PathVariable String ratingModelId) {
        Tenant tenant = MultiTenantContext.getTenant();
        RatingModelAndActionDTO ratingModelAndAction;
        try {
            ratingModelAndAction = ratingEngineProxy.updateRatingModelAndActionDTO(tenant.getId(), ratingEngineId,
                    ratingModelId, ratingModel);
        } catch (Exception ex) {
            throw graphDependencyToUIActionUtil.handleExceptionForCreateOrUpdate(ex, LedpCode.LEDP_40041);
        }

        Action action = ratingModelAndAction.getAction();
        registerAction(action, tenant);
        return ratingModelAndAction.getRatingModel();
    }

    @GetMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/metadata", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get Metadata for a given AIModel's iteration")
    public Map<String, List<ColumnMetadata>> getIterationMetadata(@PathVariable String ratingEngineId,
            @PathVariable String ratingModelId) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getIterationMetadata(tenant.getId(), ratingEngineId, ratingModelId);
    }

    @PostMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/setScoringIteration")
    @ResponseBody
    @ApiOperation(value = "Set the given ratingmodel as the Scoring Iteration for the given rating engine")
    public void setScoringIteration(@PathVariable String ratingEngineId, //
            @PathVariable String ratingModelId, @RequestBody(required = false) List<BucketMetadata> bucketMetadatas) {
        Tenant tenant = MultiTenantContext.getTenant();
        ratingEngineProxy.setScoringIteration(tenant.getId(), ratingEngineId, ratingModelId, bucketMetadatas,
                MultiTenantContext.getEmailAddress());
    }

    private void registerAction(Action action, Tenant tenant) {
        if (action != null) {
            action.setTenant(tenant);
            action.setActionInitiator(MultiTenantContext.getEmailAddress());
            log.info(String.format("Registering action %s", action));
            ActionConfiguration actionConfig = action.getActionConfiguration();
            if (actionConfig != null) {
                action.setDescription(actionConfig.serialize());
            }
            actionService.create(action);
        }
    }

    @GetMapping(value = "/{ratingEngineId}/notes")
    @ResponseBody
    @ApiOperation(value = "Get all notes for single rating engine via rating engine id.")
    public List<RatingEngineNote> getAllNotes(@PathVariable String ratingEngineId) {
        Tenant tenant = MultiTenantContext.getTenant();
        log.info(String.format("get all ratingEngineNotes by ratingEngineId=%s, tenant=%s", ratingEngineId,
                tenant.getId()));
        return ratingEngineProxy.getAllNotes(tenant.getId(), ratingEngineId);
    }

    @GetMapping(value = "/{ratingEngineId}/dependencies")
    @ResponseBody
    @ApiOperation(value = "Get all the dependencies for single rating engine via rating engine id.")
    public Map<String, List<String>> getRatingEngineDependencies(@PathVariable String ratingEngineId) {
        Tenant tenant = MultiTenantContext.getTenant();
        log.info(String.format("get all ratingEngine dependencies for ratingEngineId=%s", ratingEngineId));
        return ratingEngineProxy.getRatingEngineDependencies(tenant.getId(), ratingEngineId);
    }

    @GetMapping(value = "/{ratingEngineId}/dependencies/modelAndView")
    @ResponseBody
    @ApiOperation(value = "Get all the dependencies for single rating engine via rating engine id.")
    public ModelAndView getRatingEnigneDependenciesModelAndView(@PathVariable String ratingEngineId) {
        Tenant tenant = MultiTenantContext.getTenant();
        log.info(String.format("get all ratingEngine dependencies for ratingEngineId=%s", ratingEngineId));
        Map<String, List<String>> dependencies = ratingEngineProxy.getRatingEngineDependencies(tenant.getId(),
                ratingEngineId);
        MappingJackson2JsonView jsonView = new MappingJackson2JsonView();
        String message = graphDependencyToUIActionUtil.generateHtmlMsg(dependencies, "This model is in use.", null);
        UIAction uiAction = graphDependencyToUIActionUtil.generateUIAction("Model In Use", View.Banner, Status.Warning,
                message);
        return new ModelAndView(jsonView, ImmutableMap.of(UIAction.class.getSimpleName(), uiAction));
    }

    @PostMapping(value = "/{ratingEngineId}/notes")
    @ResponseBody
    @ApiOperation(value = "Insert one note for a certain rating engine.")
    public Boolean createNote(@PathVariable String ratingEngineId, @RequestBody NoteParams noteParams) {
        Tenant tenant = MultiTenantContext.getTenant();
        log.info(String.format("RatingEngineId=%s's note createdUser=%s, tenant=%s", ratingEngineId,
                noteParams.getUserName(), tenant.getId()));
        return ratingEngineProxy.createNote(tenant.getId(), ratingEngineId, noteParams);
    }

    @DeleteMapping(value = "/{ratingEngineId}/notes/{noteId}")
    @ResponseBody
    @ApiOperation(value = "Delete a note from a certain rating engine via rating engine id and note id.")
    public void deleteNote(@PathVariable String ratingEngineId, @PathVariable String noteId) {
        Tenant tenant = MultiTenantContext.getTenant();
        log.info(String.format("RatingEngineNoteId=%s deleted by user=%s, tenant=%s", noteId,
                MultiTenantContext.getEmailAddress(), tenant.getId()));
        ratingEngineProxy.deleteNote(tenant.getId(), ratingEngineId, noteId);
    }

    @PostMapping(value = "/{ratingEngineId}/notes/{noteId}")
    @ResponseBody
    @ApiOperation(value = "Update the content of a certain note via note id.")
    public Boolean updateNote(@PathVariable String ratingEngineId, @PathVariable String noteId,
            @RequestBody NoteParams noteParams) {
        Tenant tenant = MultiTenantContext.getTenant();
        log.info(String.format("RatingEngineNoteId=%s update by %s, tenant=%s", noteId, noteParams.getUserName(),
                tenant.getId()));
        return ratingEngineProxy.updateNote(tenant.getId(), ratingEngineId, noteId, noteParams);
    }

    @PostMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/modelingquery")
    @ResponseBody
    @ApiOperation(value = "Return a EventFrontEndQuery corresponding to the given rating engine, rating model and modelingquerytype")
    public EventFrontEndQuery getModelingQuery(@PathVariable String ratingEngineId, @PathVariable String ratingModelId,
            @RequestParam(value = "querytype", required = true) ModelingQueryType modelingQueryType,
            @RequestBody RatingEngine ratingEngine) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getModelingQueryByRating(tenant.getId(), ratingEngineId, ratingModelId,
                modelingQueryType, ratingEngine);
    }

    @PostMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/modelingquery/count")
    @ResponseBody
    @ApiOperation(value = "Return a the number of results for the modelingquerytype corresponding to the given rating engine, rating model")
    public Long getModelingQueryCount(@PathVariable String ratingEngineId, @PathVariable String ratingModelId,
            @RequestParam(value = "querytype", required = true) ModelingQueryType modelingQueryType,
            @RequestBody RatingEngine ratingEngine) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getModelingQueryCountByRating(tenant.getId(), ratingEngineId, ratingModelId,
                modelingQueryType, ratingEngine);
    }

    @PostMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/model")
    @ResponseBody
    @ApiOperation(value = "Kick off modeling job for a Rating Engine AI model and return the job id. Returns the job id if the modeling job already exists.")
    public String ratingEngineModel(@PathVariable String ratingEngineId, @PathVariable String ratingModelId,
            @RequestBody(required = false) Map<String, List<ColumnMetadata>> attributes) {
        try {
            Tenant tenant = MultiTenantContext.getTenant();
            return ratingEngineProxy.modelRatingEngine(tenant.getId(), ratingEngineId, ratingModelId, attributes,
                    MultiTenantContext.getEmailAddress());
        } catch (LedpException e) {
            throw e;
        } catch (Exception ex) {
            log.error("Failed to begin modeling job due to an unknown error!", ex);
            throw new RuntimeException(
                    "Failed to begin modeling job due to an unknown error, contact Lattice support for details!");
        }
    }

    @GetMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/model/validate")
    @ResponseBody
    @ApiOperation(value = "Validate whether the given RatingModel of the Rating Engine is valid for modeling")
    public boolean validateForModeling(@PathVariable String ratingEngineId, //
            @PathVariable String ratingModelId) {
        try {
            Tenant tenant = MultiTenantContext.getTenant();
            return ratingEngineProxy.validateForModelingByRatingEngineId(tenant.getId(), ratingEngineId, ratingModelId);
        } catch (LedpException e) {
            throw e;
        } catch (Exception ex) {
            log.error("Failed to validate due to an unknown server error.", ex);
            throw new RuntimeException("Unable to validate due to an unknown server error");
        }
    }

    @PostMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/model/validate")
    @ResponseBody
    @ApiOperation(value = "Validate whether the given RatingModel of the Rating Engine is valid for modeling")
    public boolean validateForModeling(@PathVariable String customerSpace, //
            @PathVariable String ratingEngineId, //
            @PathVariable String ratingModelId, //
            @RequestBody RatingEngine ratingEngine) {
        RatingModel ratingModel;
        ratingModel = ratingEngine.getLatestIteration();
        if (ratingModel == null || !(ratingModel instanceof AIModel)) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { "LatestIteration of the given Model is Null or unsupported for validation" });
        }
        try {
            Tenant tenant = MultiTenantContext.getTenant();
            return ratingEngineProxy.validateForModeling(tenant.getId(), ratingEngineId, ratingModelId, ratingEngine);
        } catch (LedpException e) {
            throw e;
        } catch (Exception ex) {
            log.error("Failed to validate due to an unknown server error.", ex);
            throw new RuntimeException("Unable to validate due to an unknown server error");
        }
    }
}
