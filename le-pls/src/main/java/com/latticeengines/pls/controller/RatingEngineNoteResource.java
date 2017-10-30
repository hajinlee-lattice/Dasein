package com.latticeengines.pls.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;
import com.latticeengines.pls.service.RatingEngineNoteService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "ratingenginenotes", description = "REST resource for CRUD operations of rating engine notes")
@RestController
@RequestMapping("/ratingenginenotes")
@PreAuthorize("hasRole('View_PLS_RatingEngines')")
public class RatingEngineNoteResource {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineNoteResource.class);

    @Autowired
    private RatingEngineNoteService ratingEngineNoteService;

    @RequestMapping(value = "/{ratingEngineId}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get all notes for single rating engine via rating engine id.")
    public List<RatingEngineNote> getAllNotes(@PathVariable String ratingEngineId) {
        log.info(String.format("get all ratingEngineNotes by ratingEngineId=%s", ratingEngineId));
        return ratingEngineNoteService.getAllByRatingEngineId(ratingEngineId);
    }

    @RequestMapping(value = "/{ratingEngineId}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Insert one note for a certain rating engine.")
    public boolean createNote(@PathVariable String ratingEngineId, @RequestBody NoteParams noteParams) {
        log.info(String.format("RatingEngineId=%s's note createdUser=%s", ratingEngineId, noteParams.getUserName()));
        ratingEngineNoteService.create(ratingEngineId, noteParams);
        return true;
    }

    @RequestMapping(value = "/{ratingEngineId}/{noteId}", method = RequestMethod.DELETE)
    @ResponseBody
    @ApiOperation(value = "Delete a note from a certain rating engine via rating engine id and note id.")
    public void deleteNote(@PathVariable String ratingEngineId, @PathVariable String noteId) {
        log.info(String.format("RatingEngineNoteId=%s deleted by user=%s", noteId,
                MultiTenantContext.getEmailAddress()));
        ratingEngineNoteService.deleteById(noteId);
    }

    @RequestMapping(value = "/{ratingEngineId}/{noteId}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Update the content of a certain note via note id.")
    public boolean updateNote(@PathVariable String ratingEngineId, @PathVariable String noteId,
            @RequestBody NoteParams noteParams) {
        log.info(String.format("RatingEngineNoteId=%s update by %s", noteId, noteParams.getUserName()));
        ratingEngineNoteService.updateById(noteId, noteParams);
        return true;
    }

}
