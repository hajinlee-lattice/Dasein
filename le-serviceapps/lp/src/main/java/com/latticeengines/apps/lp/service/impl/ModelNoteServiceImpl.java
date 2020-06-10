package com.latticeengines.apps.lp.service.impl;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.lp.entitymgr.ModelNoteEntityMgr;
import com.latticeengines.apps.lp.service.ModelNoteService;
import com.latticeengines.apps.lp.service.ModelSummaryService;
import com.latticeengines.domain.exposed.pls.ModelNote;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.NoteParams;

@Component("modelNoteService")
public class ModelNoteServiceImpl implements ModelNoteService {

    @Inject
    private ModelSummaryService modelSummaryService;

    @Inject
    private ModelNoteEntityMgr modelNoteEntityMgr;

    @Override
    public void create(String modelSummaryId, NoteParams noteParams) {
        ModelNote modelNote = new ModelNote();
        modelNote.setCreatedByUser(noteParams.getUserName());
        modelNote.setLastModifiedByUser(noteParams.getUserName());

        Long nowTimestamp = (new Date()).getTime();
        modelNote.setCreationTimestamp(nowTimestamp);
        modelNote.setLastModificationTimestamp(nowTimestamp);
        modelNote.setNotesContents(noteParams.getContent());

        ModelSummary summary1 = modelSummaryService.getModelSummaryByModelId(modelSummaryId);
        modelNote.setModelSummary(summary1);
        modelNote.setOrigin(noteParams.getOrigin());
        modelNote.setId(UUID.randomUUID().toString());
        modelNoteEntityMgr.create(modelNote);
    }

    @Override
    public void deleteById(String id) {
        modelNoteEntityMgr.removeById(id);
    }

    @Override
    public void updateById(String id, NoteParams noteParams) {
        ModelNote note = modelNoteEntityMgr.getByNoteId(id);
        note.setNotesContents(noteParams.getContent());
        note.setLastModificationTimestamp((new Date()).getTime());
        note.setLastModifiedByUser(noteParams.getUserName());
        modelNoteEntityMgr.update(note);
    }

    @Override
    public List<ModelNote> getAllByModelSummaryId(String modelId, boolean returnRelational, boolean returnDocument, boolean validOnly) {
        ModelSummary summary = modelSummaryService.findByModelId(modelId, returnRelational, returnDocument, validOnly);
        return modelNoteEntityMgr.getAllByModelSummaryId(String.valueOf(summary.getPid()));
    }

    @Override
    public void copyNotes(String sourceModelSummaryId, String targetModelSummaryId) {
        ModelSummary sourceModelSummary = modelSummaryService.getModelSummaryByModelId(sourceModelSummaryId);
        List<ModelNote> notes = modelNoteEntityMgr.getAllByModelSummaryId(String.valueOf(sourceModelSummary.getPid()));
        ModelSummary targetModelSummary = modelSummaryService.getModelSummaryByModelId(targetModelSummaryId);
        for (ModelNote note : notes) {
            ModelNote copyNote = new ModelNote();
            copyNote.setCreatedByUser(note.getCreatedByUser());
            copyNote.setCreationTimestamp(note.getCreationTimestamp());
            copyNote.setLastModificationTimestamp(note.getLastModificationTimestamp());
            copyNote.setLastModifiedByUser(note.getLastModifiedByUser());
            copyNote.setNotesContents(note.getNotesContents());
            copyNote.setOrigin(note.getOrigin());
            copyNote.setParentModelId(sourceModelSummaryId);
            copyNote.setModelSummary(targetModelSummary);
            copyNote.setId(UUID.randomUUID().toString());
            modelNoteEntityMgr.create(copyNote);
        }
    }
}
