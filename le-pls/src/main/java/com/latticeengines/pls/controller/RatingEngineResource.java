package com.latticeengines.pls.controller;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.cdl.RatingEngineDependencyType;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionConfiguration;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineAndActionDTO;
import com.latticeengines.domain.exposed.pls.RatingEngineDashboard;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingModelAndActionDTO;
import com.latticeengines.domain.exposed.pls.RatingsCountRequest;
import com.latticeengines.domain.exposed.pls.RatingsCountResponse;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.ActionService;
import com.latticeengines.pls.service.RatingCoverageService;
import com.latticeengines.pls.service.RatingEngineDashboardService;
import com.latticeengines.pls.service.RatingEntityPreviewService;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "ratingengine", description = "REST resource for rating engine")
@RestController
@RequestMapping("/ratingengines")
@PreAuthorize("hasRole('View_PLS_RatingEngines')")
public class RatingEngineResource {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineResource.class);

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private RatingEngineDashboardService ratingEngineDashboardService;

    @Inject
    private RatingCoverageService ratingCoverageService;

    @Inject
    private RatingEntityPreviewService ratingEntityPreviewService;

    @Inject
    private ActionService actionService;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all Rating Engine summaries for a tenant")
    public List<RatingEngineSummary> getRatingEngineSummaries( //
            @RequestParam(value = "status", required = false) String status, //
            @RequestParam(value = "type", required = false) String type) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getRatingEngineSummaries(tenant.getId(), status, type);
    }

    @RequestMapping(value = "/types", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get types for Rating Engines")
    public List<RatingEngineType> getRatingEngineTypes() {
        return Arrays.asList(RatingEngineType.values());
    }

    @RequestMapping(value = "/{ratingEngineId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a Rating Engine given its id")
    public RatingEngine getRatingEngine( //
            @PathVariable String ratingEngineId, //
            HttpServletRequest request, //
            HttpServletResponse response) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getRatingEngine(tenant.getId(), ratingEngineId);
    }

    @RequestMapping(value = "/{ratingEngineId}/entitypreview", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get preview of Account and Contact as related to Rating Engine given its id")
    public DataPage getEntityPreview( //
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
            @RequestParam(value = "selectedBuckets", required = false) List<String> selectedBuckets) {
        RatingEngine ratingEngine = getRatingEngine(ratingEngineId, null, null);

        descending = descending == null ? false : descending;
        restrictNotNullSalesforceId = restrictNotNullSalesforceId == null ? false : restrictNotNullSalesforceId;
        return ratingEntityPreviewService.getEntityPreview(ratingEngine, offset, maximum, entityType, sortBy,
                descending, bucketFieldName, lookupFieldNames, restrictNotNullSalesforceId, freeFormTextSearch,
                selectedBuckets);
    }

    @RequestMapping(value = "/{ratingEngineId}/dashboard", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get dashboard info for Rating Engine given its id")
    public RatingEngineDashboard getRatingsDashboard(@PathVariable String ratingEngineId) {
        return ratingEngineDashboardService.getRatingsDashboard(ratingEngineId);
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Register or update a Rating Engine")
    @PreAuthorize("hasRole('Create_PLS_RatingEngines')")
    public RatingEngine createOrUpdateRatingEngine(@RequestBody RatingEngine ratingEngine) {
        Tenant tenant = MultiTenantContext.getTenant();
        RatingEngineAndActionDTO ratingEngineAndAction = ratingEngineProxy
                .createOrUpdateRatingEngineAndActionDTO(tenant.getId(), ratingEngine);
        Action action = ratingEngineAndAction.getAction();
        registerAction(action, tenant);
        return ratingEngineAndAction.getRatingEngine();
    }

    @RequestMapping(value = "/{ratingEngineId}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a Rating Engine given its id")
    @PreAuthorize("hasRole('Edit_PLS_RatingEngines')")
    public Boolean deleteRatingEngine(@PathVariable String ratingEngineId) {
        Tenant tenant = MultiTenantContext.getTenant();
        ratingEngineProxy.deleteRatingEngine(tenant.getId(), ratingEngineId);
        return true;
    }

    @RequestMapping(value = "/coverage", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get CoverageInfo for ids in Rating count request")
    public RatingsCountResponse getRatingEngineCoverageInfo(@RequestBody RatingsCountRequest ratingModelSegmentIds) {
        return ratingCoverageService.getCoverageInfo(ratingModelSegmentIds);
    }

    @RequestMapping(value = "/{ratingEngineId}/ratingmodels", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get Rating Models associated with a Rating Engine given its id")
    public List<RatingModel> getRatingModels(@PathVariable String ratingEngineId) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getRatingModels(tenant.getId(), ratingEngineId);
    }

    @RequestMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a particular Rating Model associated with a Rating Engine given its Rating Engine id and Rating Model id")
    public RatingModel getRatingModel(@PathVariable String ratingEngineId, @PathVariable String ratingModelId) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getRatingModel(tenant.getId(), ratingEngineId, ratingModelId);
    }

    @RequestMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a particular Rating Model associated with a Rating Engine given its Rating Engine id and Rating Model id")
    public RatingModel updateRatingModel(@RequestBody RatingModel ratingModel, @PathVariable String ratingEngineId,
            @PathVariable String ratingModelId) {
        Tenant tenant = MultiTenantContext.getTenant();
        RatingModelAndActionDTO ratingModelAndAction = ratingEngineProxy.updateRatingModelAndActionDTO(tenant.getId(),
                ratingEngineId, ratingModelId, ratingModel);
        Action action = ratingModelAndAction.getAction();
        registerAction(action, tenant);
        return ratingModelAndAction.getRatingModel();
    }

    private void registerAction(Action action, Tenant tenant) {
        if (action != null) {
            action.setTenant(tenant);
            log.info(String.format("Registering action %s", action));
            ActionConfiguration actionConfig = action.getActionConfiguration();
            if (actionConfig != null) {
                action.setDescription(actionConfig.serialize());
            }
            actionService.create(action);
        }
    }

    @RequestMapping(value = "/{ratingEngineId}/notes", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get all notes for single rating engine via rating engine id.")
    public List<RatingEngineNote> getAllNotes(@PathVariable String ratingEngineId) {
        Tenant tenant = MultiTenantContext.getTenant();
        log.info(String.format("get all ratingEngineNotes by ratingEngineId=%s, tenant=%s", ratingEngineId,
                tenant.getId()));
        return ratingEngineProxy.getAllNotes(tenant.getId(), ratingEngineId);
    }

    @RequestMapping(value = "/{ratingEngineId}/dependencies", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get all the dependencies for single rating engine via rating engine id.")
    public Map<RatingEngineDependencyType, List<String>> getRatingEngigneDependencies(
            @PathVariable String ratingEngineId) {
        Tenant tenant = MultiTenantContext.getTenant();
        log.info(String.format("get all ratingEngine dependencies for ratingEngineId=%s", ratingEngineId));
        return ratingEngineProxy.getRatingEngineDependencies(tenant.getId(), ratingEngineId);
    }

    @RequestMapping(value = "/{ratingEngineId}/notes", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Insert one note for a certain rating engine.")
    public Boolean createNote(@PathVariable String ratingEngineId, @RequestBody NoteParams noteParams) {
        Tenant tenant = MultiTenantContext.getTenant();
        log.info(String.format("RatingEngineId=%s's note createdUser=%s, tenant=%s", ratingEngineId,
                noteParams.getUserName(), tenant.getId()));
        return ratingEngineProxy.createNote(tenant.getId(), ratingEngineId, noteParams);
    }

    @RequestMapping(value = "/{ratingEngineId}/notes/{noteId}", method = RequestMethod.DELETE)
    @ResponseBody
    @ApiOperation(value = "Delete a note from a certain rating engine via rating engine id and note id.")
    public void deleteNote(@PathVariable String ratingEngineId, @PathVariable String noteId) {
        Tenant tenant = MultiTenantContext.getTenant();
        log.info(String.format("RatingEngineNoteId=%s deleted by user=%s, tenant=%s", noteId,
                MultiTenantContext.getEmailAddress(), tenant.getId()));
        ratingEngineProxy.deleteNote(tenant.getId(), ratingEngineId, noteId);
    }

    @RequestMapping(value = "/{ratingEngineId}/notes/{noteId}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Update the content of a certain note via note id.")
    public Boolean updateNote(@PathVariable String ratingEngineId, @PathVariable String noteId,
            @RequestBody NoteParams noteParams) {
        Tenant tenant = MultiTenantContext.getTenant();
        log.info(String.format("RatingEngineNoteId=%s update by %s, tenant=%s", noteId, noteParams.getUserName(),
                tenant.getId()));
        return ratingEngineProxy.updateNote(tenant.getId(), ratingEngineId, noteId, noteParams);
    }

    @RequestMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/modelingquery", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Return a EventFrontEndQuery corresponding to the given rating engine, rating model and modelingquerytype")
    public EventFrontEndQuery getModelingQuery(@PathVariable String ratingEngineId, @PathVariable String ratingModelId,
            @RequestParam(value = "querytype", required = true) ModelingQueryType modelingQueryType,
            @RequestBody RatingEngine ratingEngine) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getModelingQueryByRating(tenant.getId(), ratingEngineId, ratingModelId,
                modelingQueryType, ratingEngine);
    }

    @RequestMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/modelingquery/count", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Return a the number of results for the modelingquerytype corresponding to the given rating engine, rating model")
    public Long getModelingQueryCount(@PathVariable String ratingEngineId, @PathVariable String ratingModelId,
            @RequestParam(value = "querytype", required = true) ModelingQueryType modelingQueryType,
            @RequestBody RatingEngine ratingEngine) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getModelingQueryCountByRating(tenant.getId(), ratingEngineId, ratingModelId,
                modelingQueryType, ratingEngine);
    }

    @RequestMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/model", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Kick off modeling job for a Rating Engine AI model and return the job id. Returns the job id if the modeling job already exists.")
    public String ratingEngineModel(@PathVariable String ratingEngineId, @PathVariable String ratingModelId) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.modelRatingEngine(tenant.getId(), ratingEngineId, ratingModelId,
                MultiTenantContext.getEmailAddress());
    }
}
