package com.latticeengines.pls.service.impl;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.ModelNote;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.pls.entitymanager.ModelNoteEntityMgr;
import com.latticeengines.pls.service.ModelNoteService;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;

@Component("modelNoteService")
public class ModelNoteServiceImpl implements ModelNoteService {

    @Inject
    private ModelNoteEntityMgr modelNotesEntityMgr;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Override
    public ModelNote findById(String id) {
        return modelNotesEntityMgr.findByNoteId(id);
    }

    @Override
    public void create(String modelSummaryId, NoteParams noteParams) {
        ModelNote modelNote = new ModelNote();
        modelNote.setCreatedByUser(noteParams.getUserName());
        modelNote.setLastModifiedByUser(noteParams.getUserName());

        Long nowTimestamp = (new Date()).getTime();
        modelNote.setCreationTimestamp(nowTimestamp);
        modelNote.setLastModificationTimestamp(nowTimestamp);
        modelNote.setNotesContents(noteParams.getContent());

        ModelSummary summary1 = modelSummaryProxy.getByModelId(modelSummaryId);
        modelNote.setModelSummary(summary1);
        modelNote.setOrigin(noteParams.getOrigin());
        modelNote.setId(UUID.randomUUID().toString());
        modelNotesEntityMgr.create(modelNote);
    }

    @Override
    public void deleteById(String id) {
        modelNotesEntityMgr.deleteById(id);
    }

    @Override
    public void updateById(String id, NoteParams noteParams) {
        ModelNote note = modelNotesEntityMgr.findByNoteId(id);
        note.setNotesContents(noteParams.getContent());
        note.setLastModificationTimestamp((new Date()).getTime());
        note.setLastModifiedByUser(noteParams.getUserName());
        modelNotesEntityMgr.update(note);
    }

    @Override
    public List<ModelNote> getAllByModelSummaryId(String modelSummaryId) {
        ModelSummary summary1 = modelSummaryProxy.findByModelId(MultiTenantContext.getTenant().getId(),
                modelSummaryId, false, false, true);
        return modelNotesEntityMgr.getAllByModelSummaryId(String.valueOf(summary1.getPid()));
    }

    @Override
    public void copyNotes(String sourceModelSummaryId, String targetModelSummaryId) {
        ModelSummary sourceModelSummary = modelSummaryProxy.getByModelId(sourceModelSummaryId);
        List<ModelNote> notes = modelNotesEntityMgr.getAllByModelSummaryId(String.valueOf(sourceModelSummary.getPid()));
        ModelSummary targetModelSummary = modelSummaryProxy.getByModelId(targetModelSummaryId);
        for (ModelNote note : notes) {
            System.out.println("content" + note.getNotesContents());
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
            modelNotesEntityMgr.create(copyNote);
        }
    }
}
