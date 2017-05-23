package com.latticeengines.pls.controller;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.pls.ModelNotes;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.security.exposed.util.MultiTenantContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modelnotes", description = "REST resource for interacting with model notes")
@RestController
@RequestMapping("/modelnotes")
@PreAuthorize("hasRole('View_PLS_Data')")
public class ModelNotesResource {

    private static final Logger log = Logger.getLogger(ModelNotesResource.class);

//    @Autowired
//    private ModelNotesService modelNotesService;

    @RequestMapping(value = "/{modelSummaryId}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get all notes for single model summary.")
    public List<ModelNotes> getAllNotes(@PathVariable String modelSummaryId) {
        log.debug(String.format("get all modelNotes by ModelSummaryId %s", modelSummaryId));
//        return modelNotesService.getAllByModelSummaryId(modelSummaryId);
        return null;
    }

    @RequestMapping(value = "/{modelSummaryId}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Insert one note for certain model summary.")
    public boolean createNote(@PathVariable String modelSummaryId, @RequestBody NoteParams noteParams) {
        log.debug(String.format("ModelSummary %s's ModelNotes created by %s", modelSummaryId, noteParams.getUserName()));
        //modelNotesService.create(modelSummaryId, noteParams);
        return true;
    }

    @RequestMapping(value = "/{modelSummaryId}/{noteId}", method = RequestMethod.DELETE)
    @ResponseBody
    @ApiOperation(value = "Delete one note from certain model summary.")
    public boolean deleteNote(@PathVariable String modelSummaryId, @PathVariable String noteId) {
        log.debug(String.format("ModelNotes %s deleted by user %s", noteId, MultiTenantContext.getEmailAddress()));
        //modelNotesService.deleteByNoteId(noteId);
        return true;
    }

    @RequestMapping(value = "/{modelSummaryId}/{noteId}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Update the content of one certain note.")
    public boolean updateNote(@PathVariable String modelSummaryId, @PathVariable String noteId,
                              @RequestBody NoteParams noteParams) {
        log.debug(String.format("ModelNotes %s update by %s", noteId, noteParams.getUserName()));
        //modelNotesService.updateByNoteId(noteId, noteParams);
        return true;
    }

    @RequestMapping(value = "/{fromModelSummaryId}/{toModelSummaryId}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Insert one note for certain model summary.")
    public boolean copyNotes(@PathVariable String fromModelSummaryId, @PathVariable String toModelSummaryId) {
        log.debug(String.format("Copy notes from ModelSummary %s to ModelSummary %s ModelNotes", fromModelSummaryId, toModelSummaryId));
        //modelNotesService.copyNotes(fromModelSummaryId, toModelSummaryId);
        return true;
    }
}
