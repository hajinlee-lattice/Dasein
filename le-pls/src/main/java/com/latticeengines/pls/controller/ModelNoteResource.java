package com.latticeengines.pls.controller;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.ModelNote;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.pls.service.ModelNoteService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modelnotes", description = "REST resource for interacting with model notes")
@RestController
@RequestMapping("/modelnotes")
@PreAuthorize("hasRole('View_PLS_Data')")
public class ModelNoteResource {

    private static final Logger log = LoggerFactory.getLogger(ModelNoteResource.class);

    @Inject
    private ModelNoteService modelNoteService;

    @GetMapping("/{modelSummaryId}")
    @ResponseBody
    @ApiOperation(value = "Get all notes for single model summary.")
    public List<ModelNote> getAllNotes(@PathVariable String modelSummaryId) {
        log.debug(String.format("get all modelNotes by ModelSummaryId %s", modelSummaryId));
        return modelNoteService.getAllByModelSummaryId(modelSummaryId);
    }

    @PostMapping("/{modelSummaryId}")
    @ResponseBody
    @ApiOperation(value = "Insert one note for certain model summary.")
    public boolean createNote(@PathVariable String modelSummaryId, @RequestBody NoteParams noteParams) {
        log.debug(
                String.format("ModelSummary %s's ModelNote created by %s", modelSummaryId, noteParams.getUserName()));
        modelNoteService.create(modelSummaryId, noteParams);
        return true;
    }

    @DeleteMapping("/{modelSummaryId}/{noteId}")
    @ResponseBody
    @ApiOperation(value = "Delete one note from certain model summary.")
    public boolean deleteNote(@PathVariable String modelSummaryId, @PathVariable String noteId) {
        log.debug(String.format("ModelNote %s deleted by user %s", noteId, MultiTenantContext.getEmailAddress()));
        modelNoteService.deleteById(noteId);
        return true;
    }

    @PostMapping("/{modelSummaryId}/{noteId}")
    @ResponseBody
    @ApiOperation(value = "Update the content of one certain note.")
    public boolean updateNote(@PathVariable String modelSummaryId, @PathVariable String noteId,
            @RequestBody NoteParams noteParams) {
        log.debug(String.format("ModelNote %s update by %s", noteId, noteParams.getUserName()));
        modelNoteService.updateById(noteId, noteParams);
        return true;
    }

}
