package com.latticeengines.pls.controller;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.latticeengines.domain.exposed.pls.RatingEngineModelingParameters;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineDashboard;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingsCountRequest;
import com.latticeengines.domain.exposed.pls.RatingsCountResponse;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryDownloadFlagEntityMgr;
import com.latticeengines.pls.service.RatingCoverageService;
import com.latticeengines.pls.service.RatingEngineDashboardService;
import com.latticeengines.pls.service.RatingEntityPreviewService;
import com.latticeengines.pls.workflow.RatingEngineImportMatchAndModelWorkflowSubmitter;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

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

    @Autowired
    private RatingEngineDashboardService ratingEngineDashboardService;

    @Autowired
    private RatingCoverageService ratingCoverageService;

    @Autowired
    private RatingEntityPreviewService ratingEntityPreviewService;

    @Autowired
    private ModelSummaryDownloadFlagEntityMgr modelSummaryDownloadFlagEntityMgr;

    @Autowired
    private RatingEngineImportMatchAndModelWorkflowSubmitter ratingEngineImportMatchAndModelWorkflowSubmitter;

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
    public RatingEngine createRatingEngine(@RequestBody RatingEngine ratingEngine) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.createOrUpdateRatingEngine(tenant.getId(), ratingEngine);
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
        return ratingEngineProxy.updateRatingModel(tenant.getId(), ratingEngineId, ratingModelId, ratingModel);
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

    @RequestMapping(value = "/{ratingEngineId}/ratingModels/{ratingModelId}/model", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Generate a Rating Engine model from the table name(or query) and parameters. Returns the job id.")
    public ResponseDocument<String> ratingEngineModel(@PathVariable String ratingEngineId,
            @PathVariable String ratingModelId) {
        Tenant tenant = MultiTenantContext.getTenant();
        RatingEngine ratingEngine = ratingEngineProxy.getRatingEngine(tenant.getId(),ratingEngineId);
        RatingModel ratingModel = ratingEngineProxy.getRatingModel(tenant.getId(), ratingEngineId, ratingModelId);

        if (ratingModel instanceof AIModel) {
            modelSummaryDownloadFlagEntityMgr.addDownloadFlag(MultiTenantContext.getTenant().getId());
            RatingEngineModelingParameters parameters =new RatingEngineModelingParameters();
            parameters.setUserId(MultiTenantContext.getEmailAddress());
            EventFrontEndQuery eventQuery = EventFrontEndQuery.fromFrontEndQuery(FrontEndQuery.fromSegment(ratingEngine.getSegment()));
            eventQuery.setTargetProductIds(((AIModel) ratingModel).getTargetProducts());
            parameters.setTargetFilterQuery(eventQuery);
            parameters.setTrainFilterQuery(eventQuery);
            parameters.setEventFilterQuery(eventQuery);

            log.info(String.format("Rating Engine model called with parameters %s", parameters.toString()));
            return ResponseDocument.successResponse( //
                    ratingEngineImportMatchAndModelWorkflowSubmitter.submit(parameters).toString());
        } else {
            throw new LedpException(LedpCode.LEDP_31107, new String[] { ratingModel.getClass().getName() });
        }
    }
}
