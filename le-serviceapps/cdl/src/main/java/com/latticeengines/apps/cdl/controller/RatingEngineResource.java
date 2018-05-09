package com.latticeengines.apps.cdl.controller;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.annotation.Action;
import com.latticeengines.apps.cdl.service.RatingCoverageService;
import com.latticeengines.apps.cdl.service.RatingEngineDashboardService;
import com.latticeengines.apps.cdl.service.RatingEngineNoteService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.service.RatingEntityPreviewService;
import com.latticeengines.apps.cdl.util.ActionContext;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.cdl.RatingEngineDependencyType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineAndActionDTO;
import com.latticeengines.domain.exposed.pls.RatingEngineDashboard;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingModelAndActionDTO;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.ratings.coverage.RatingsCountRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingsCountResponse;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "ratingengine", description = "REST resource for rating engine")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/ratingengines")
public class RatingEngineResource {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineResource.class);

    private final RatingEngineService ratingEngineService;

    private final RatingEngineNoteService ratingEngineNoteService;

    private final RatingCoverageService ratingCoverageService;

    private final RatingEngineDashboardService ratingEngineDashboardService;

    private final RatingEntityPreviewService ratingEntityPreviewService;

    @Inject
    public RatingEngineResource(RatingEngineService ratingEngineService, //
            RatingEngineNoteService ratingEngineNoteService, //
            RatingCoverageService ratingCoverageService, //
            RatingEngineDashboardService ratingEngineDashboardService, //
            RatingEntityPreviewService ratingEntityPreviewService) {
        this.ratingEngineService = ratingEngineService;
        this.ratingEngineNoteService = ratingEngineNoteService;
        this.ratingCoverageService = ratingCoverageService;
        this.ratingEngineDashboardService = ratingEngineDashboardService;
        this.ratingEntityPreviewService = ratingEntityPreviewService;
    }

    @GetMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "Get all Rating Engine summaries for a tenant")
    public List<RatingEngineSummary> getRatingEngineSummaries( //
            @PathVariable String customerSpace, @RequestParam(value = "status", required = false) String status, //
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
    public RatingEngine getRatingEngine(@PathVariable String customerSpace, @PathVariable String ratingEngineId) {
        return ratingEngineService.getRatingEngineById(ratingEngineId, true, true);
    }

    @PostMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "Register or update a Rating Engine")
    public RatingEngine createOrUpdateRatingEngine(@PathVariable String customerSpace,
            @RequestBody RatingEngine ratingEngine,
            @RequestParam(value = "unlink-segment", required = false, defaultValue = "false") Boolean unlinkSegment) {
        if (StringUtils.isEmpty(customerSpace)) {
            throw new LedpException(LedpCode.LEDP_39002);
        }
        if (ratingEngine == null) {
            throw new NullPointerException("Rating Engine is null.");
        }
        return ratingEngineService.createOrUpdate(ratingEngine, CustomerSpace.parse(customerSpace).toString(),
                unlinkSegment);
    }

    @PostMapping(value = "/with-action")
    @ResponseBody
    @Action
    @ApiOperation(value = "Register or update a Rating Engine, returns RatingEngineAndActionDTO")
    public RatingEngineAndActionDTO createOrUpdateRatingEngineAndActionDTO(@PathVariable String customerSpace,
            @RequestBody RatingEngine ratingEngine,
            @RequestParam(value = "unlink-segment", required = false, defaultValue = "false") Boolean unlinkSegment) {
        RatingEngine updatedRatingEngine = createOrUpdateRatingEngine(customerSpace, ratingEngine, unlinkSegment);
        return new RatingEngineAndActionDTO(updatedRatingEngine, ActionContext.getAction());
    }

    @DeleteMapping(value = "/{ratingEngineId}")
    @ResponseBody
    @ApiOperation(value = "Delete a Rating Engine given its id")
    public Boolean deleteRatingEngine(@PathVariable String customerSpace, @PathVariable String ratingEngineId, //
            @RequestParam(value = "hard-delete", required = false, defaultValue = "false") Boolean hardDelete) {
        ratingEngineService.deleteById(ratingEngineId, hardDelete);
        return true;
    }

    @PutMapping(value = "/{ratingEngineId}/revertdelete")
    @ResponseBody
    @ApiOperation(value = "Delete a Rating Engine given its id")
    public Boolean revertDeleteRatingEngine(@PathVariable String customerSpace, @PathVariable String ratingEngineId) {
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

    @RequestMapping(value = "/{ratingEngineId}/ratingmodels", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get Rating Models associated with a Rating Engine given its id")
    public List<RatingModel> getRatingModels(@PathVariable String customerSpace, @PathVariable String ratingEngineId) {
        return ratingEngineService.getRatingModelsByRatingEngineId(ratingEngineId);
    }

    @RequestMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a particular Rating Model associated with a Rating Engine given its Rating Engine id and Rating Model id")
    public RatingModel getRatingModel(@PathVariable String customerSpace, @PathVariable String ratingEngineId,
            @PathVariable String ratingModelId) {
        return ratingEngineService.getRatingModel(ratingEngineId, ratingModelId);
    }

    @PostMapping(value = "/with-action/{ratingEngineId}/ratingmodels/{ratingModelId}")
    @ResponseBody
    @Action
    @ApiOperation(value = "Update a particular Rating Model associated with a Rating Engine given its Rating Engine id and Rating Model id, returns RatingModelAndActionDTO")
    public RatingModelAndActionDTO updateRatingModelAndActionDTO(@PathVariable String customerSpace,
            @RequestBody RatingModel ratingModel, @PathVariable String ratingEngineId,
            @PathVariable String ratingModelId) {
        RatingModel updatedRatingModel = updateRatingModel(customerSpace, ratingModel, ratingEngineId, ratingModelId);
        return new RatingModelAndActionDTO(updatedRatingModel, ActionContext.getAction());
    }

    @RequestMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a particular Rating Model associated with a Rating Engine given its Rating Engine id and Rating Model id")
    public RatingModel updateRatingModel(@PathVariable String customerSpace, @RequestBody RatingModel ratingModel,
            @PathVariable String ratingEngineId, @PathVariable String ratingModelId) {
        return ratingEngineService.updateRatingModel(ratingEngineId, ratingModelId, ratingModel);
    }

    @RequestMapping(value = "/{ratingEngineId}/notes", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get all notes for single rating engine via rating engine id.")
    public List<RatingEngineNote> getAllNotes(@PathVariable String customerSpace, @PathVariable String ratingEngineId) {
        log.info(String.format("get all ratingEngineNotes by ratingEngineId=%s", ratingEngineId));
        return ratingEngineNoteService.getAllByRatingEngineId(ratingEngineId);
    }

    @RequestMapping(value = "/{ratingEngineId}/dependencies", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get all the dependencies for single rating engine via rating engine id.")
    public Map<RatingEngineDependencyType, List<String>> getRatingEngigneDependencies(
            @PathVariable String customerSpace, @PathVariable String ratingEngineId) {
        log.info(String.format("get all ratingEngineNotes by ratingEngineId=%s", ratingEngineId));
        return ratingEngineService.getRatingEngineDependencies(customerSpace, ratingEngineId);
    }

    @RequestMapping(value = "/{ratingEngineId}/notes", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Insert one note for a certain rating engine.")
    public Boolean createNote(@PathVariable String customerSpace, @PathVariable String ratingEngineId,
            @RequestBody NoteParams noteParams) {
        log.info(String.format("RatingEngineId=%s's note createdUser=%s", ratingEngineId, noteParams.getUserName()));
        ratingEngineNoteService.create(ratingEngineId, noteParams);
        return Boolean.TRUE;
    }

    @RequestMapping(value = "/{ratingEngineId}/notes/{noteId}", method = RequestMethod.DELETE)
    @ResponseBody
    @ApiOperation(value = "Delete a note from a certain rating engine via rating engine id and note id.")
    public void deleteNote(@PathVariable String customerSpace, @PathVariable String ratingEngineId,
            @PathVariable String noteId) {
        log.info(String.format("RatingEngineNoteId=%s deleted by user=%s", noteId,
                MultiTenantContext.getEmailAddress()));
        ratingEngineNoteService.deleteById(noteId);
    }

    @RequestMapping(value = "/{ratingEngineId}/notes/{noteId}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Update the content of a certain note via note id.")
    public Boolean updateNote(@PathVariable String customerSpace, @PathVariable String ratingEngineId,
            @PathVariable String noteId, @RequestBody NoteParams noteParams) {
        log.info(String.format("RatingEngineNoteId=%s update by %s", noteId, noteParams.getUserName()));
        ratingEngineNoteService.updateById(noteId, noteParams);
        return Boolean.TRUE;
    }

    @RequestMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/modelingquery", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Return a EventFrontEndQuery corresponding to the given rating engine, rating model and modelingquerytype")
    public EventFrontEndQuery getModelingQueryByRatingId(@PathVariable String customerSpace,
            @PathVariable String ratingEngineId, @PathVariable String ratingModelId,
            @RequestParam(value = "querytype") ModelingQueryType modelingQueryType,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        RatingEngine ratingEngine = getRatingEngine(customerSpace, ratingEngineId);
        return getModelingQueryByRating(customerSpace, ratingEngineId, ratingModelId, modelingQueryType, version,
                ratingEngine);
    }

    @RequestMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/modelingquery", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Return a EventFrontEndQuery corresponding to the given rating engine, rating model and modelingquerytype")
    public EventFrontEndQuery getModelingQueryByRating(@PathVariable String customerSpace,
            @PathVariable String ratingEngineId, @PathVariable String ratingModelId,
            @RequestParam(value = "querytype") ModelingQueryType modelingQueryType,
            @RequestParam(value = "version", required = false) DataCollection.Version version,
            @RequestBody RatingEngine ratingEngine) {
        RatingModel ratingModel;
        if (ratingEngine == null) {
            throw new LedpException(LedpCode.LEDP_40013);
        } else {
            ratingModel = ratingEngine.getActiveModel();
        }
        return ratingEngineService.getModelingQuery(customerSpace, ratingEngine, ratingModel, modelingQueryType,
                version);
    }

    @RequestMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/modelingquery/count", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Return a EventFrontEndQuery corresponding to the given rating engine, rating model and modelingquerytype")
    public Long getModelingQueryCountByRatingId(@PathVariable String customerSpace, @PathVariable String ratingEngineId,
            @PathVariable String ratingModelId,
            @RequestParam(value = "querytype", required = true) ModelingQueryType modelingQueryType,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        RatingEngine ratingEngine = getRatingEngine(customerSpace, ratingEngineId);
        return getModelingQueryCountByRating(customerSpace, ratingEngineId, ratingModelId, modelingQueryType, version,
                ratingEngine);
    }

    @RequestMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/modelingquery/count", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Return a EventFrontEndQuery corresponding to the given rating engine, rating model and modelingquerytype")
    public Long getModelingQueryCountByRating(@PathVariable String customerSpace, @PathVariable String ratingEngineId,
            @PathVariable String ratingModelId, @RequestParam(value = "querytype") ModelingQueryType modelingQueryType,

            @RequestParam(value = "version", required = false) DataCollection.Version version,
            @RequestBody RatingEngine ratingEngine) {
        RatingModel ratingModel;
        if (ratingEngine == null) {
            throw new LedpException(LedpCode.LEDP_40013);
        } else {
            ratingModel = ratingEngine.getActiveModel();
        }
        return ratingEngineService.getModelingQueryCount(customerSpace, ratingEngine, ratingModel, modelingQueryType,
                version);

    }

    @RequestMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}/model", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Kick off modeling job for a Rating Engine AI model and return the job id. Returns the job id if the modeling job already exists.")
    public String modelRatingEngine(@PathVariable String customerSpace, @PathVariable String ratingEngineId,
            @PathVariable String ratingModelId, @RequestParam(value = "useremail", required = true) String userEmail) {
        RatingEngine ratingEngine = getRatingEngine(customerSpace, ratingEngineId);
        RatingModel ratingModel = getRatingModel(customerSpace, ratingEngineId, ratingModelId);

        if (!(ratingModel instanceof AIModel)) {
            throw new LedpException(LedpCode.LEDP_31107, new String[] { ratingModel.getClass().getName() });
        }

        return ratingEngineService.modelRatingEngine(customerSpace, ratingEngine, (AIModel) ratingModel, userEmail);
    }

    @PostMapping(value = "/coverage")
    @ResponseBody
    @ApiOperation(value = "Get CoverageInfo for ids in Rating count request")
    public RatingsCountResponse getRatingEngineCoverageInfo(@PathVariable String customerSpace,
            @RequestBody RatingsCountRequest ratingModelSegmentIds) {
        return ratingCoverageService.getCoverageInfo(customerSpace, ratingModelSegmentIds);
    }

    @RequestMapping(value = "/{ratingEngineId}/dashboard", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get dashboard info for Rating Engine given its id")
    public RatingEngineDashboard getRatingEngineDashboardById(@PathVariable String customerSpace,
            @PathVariable String ratingEngineId) {
        return ratingEngineDashboardService.getRatingsDashboard(CustomerSpace.parse(customerSpace).toString(),
                ratingEngineId);
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
        RatingEngine ratingEngine = ratingEngineService.getRatingEngineById(ratingEngineId, false, false);

        descending = descending == null ? false : descending;

        return ratingEntityPreviewService.getEntityPreview(ratingEngine, offset, maximum, entityType, sortBy,
                descending, bucketFieldName, lookupFieldNames, restrictNotNullSalesforceId, freeFormTextSearch,
                selectedBuckets);
    }

    @RequestMapping(value = "/{ratingEngineId}/entitypreview/count", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get total count of Account and Contact as related to Rating Engine given its id")
    public Long getEntityPreviewCount( //
            @PathVariable String ratingEngineId, //
            @RequestParam(value = "entityType", required = true) BusinessEntity entityType, //
            @RequestParam(value = "restrictNotNullSalesforceId", required = false) Boolean restrictNotNullSalesforceId, //
            @RequestParam(value = "freeFormTextSearch", required = false) String freeFormTextSearch, //
            @RequestParam(value = "selectedBuckets", required = false) List<String> selectedBuckets) {
        RatingEngine ratingEngine = ratingEngineService.getRatingEngineById(ratingEngineId, false, false);

        return ratingEntityPreviewService.getEntityPreviewCount(ratingEngine, entityType, restrictNotNullSalesforceId,
                freeFormTextSearch, selectedBuckets);
    }
}
