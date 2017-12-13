package com.latticeengines.apps.cdl.controller;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.RatingEngineNoteService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.util.MultiTenantContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "ratingengine", description = "REST resource for rating engine")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/ratingengines")
public class RatingEngineResource {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineResource.class);

    private final RatingEngineService ratingEngineService;

    private final RatingEngineNoteService ratingEngineNoteService;

    @Inject
    public RatingEngineResource(RatingEngineService ratingEngineService,
            RatingEngineNoteService ratingEngineNoteService) {
        this.ratingEngineService = ratingEngineService;
        this.ratingEngineNoteService = ratingEngineNoteService;
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all Rating Engine summaries for a tenant")
    public List<RatingEngineSummary> getRatingEngineSummaries( //
            @PathVariable String customerSpace, @RequestParam(value = "status", required = false) String status, //
            @RequestParam(value = "type", required = false) String type) {
        return ratingEngineService.getAllRatingEngineSummariesWithTypeAndStatus(type, status);
    }

    @RequestMapping(value = "/types", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get types for Rating Engines")
    public List<RatingEngineType> getRatingEngineTypes(@PathVariable String customerSpace) {
        return Arrays.asList(RatingEngineType.values());
    }

    @RequestMapping(value = "/ids", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get ids for Rating Engines. Can be filtered by segment name.")
    public List<String> getRatingEngineIds(@PathVariable String customerSpace,
            @RequestParam(value = "segment", required = false) String segment) {
        return ratingEngineService.getAllRatingEngineIdsInSegment(segment);
    }

    @RequestMapping(value = "/{ratingEngineId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a Rating Engine given its id")
    public RatingEngine getRatingEngine(@PathVariable String customerSpace, @PathVariable String ratingEngineId) {
        return ratingEngineService.getRatingEngineById(ratingEngineId, true, true);
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Register or update a Rating Engine")
    public RatingEngine createRatingEngine(@PathVariable String customerSpace, @RequestBody RatingEngine ratingEngine) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (tenant == null) {
            log.warn("Tenant is null for the request.");
            return null;
        }
        if (ratingEngine == null) {
            throw new NullPointerException("Rating Engine is null.");
        }
        return ratingEngineService.createOrUpdate(ratingEngine, tenant.getId());
    }

    @RequestMapping(value = "/{ratingEngineId}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a Rating Engine given its id")
    public Boolean deleteRatingEngine(@PathVariable String customerSpace, @PathVariable String ratingEngineId) {
        ratingEngineService.deleteById(ratingEngineId);
        return true;
    }

    @RequestMapping(value = "/{ratingEngineId}/counts", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update rating engine counts")
    public Map<String, Long> updateRatingEngineCounts(@PathVariable String customerSpace,
            @PathVariable String ratingEngineId) {
        // TODO: to convert to PUT
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

}
