package com.latticeengines.pls.service.impl;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.ModelNotes;
import com.latticeengines.domain.exposed.pls.ModelNotesOrigin;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.pls.entitymanager.ModelNotesEntityMgr;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.service.ModelNotesService;

@Component("modelNotesService")
public class ModelNotesServiceImpl implements ModelNotesService {

    @Autowired
    private ModelNotesEntityMgr modelNotesEntityMgr;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Override
    public void create(String modelSummaryId, NoteParams noteParams) {
        ModelNotes modelNote = new ModelNotes();
        modelNote.setCreatedByUser(noteParams.getUserName());
        modelNote.setLastModifiedByUser(noteParams.getUserName());

        Long nowTimestamp = (new Date()).getTime();
        modelNote.setCreationTimestamp(nowTimestamp);
        modelNote.setLastModificationTimestamp(nowTimestamp);
        modelNote.setNotesContents(noteParams.getContent());
        ModelSummary summary1 = modelSummaryEntityMgr.getByModelId(modelSummaryId);
        modelNote.setModelSummary(summary1);
        modelNote.setOrigin(noteParams.getOrigin());
        modelNote.setId(UUID.randomUUID().toString());
        modelNotesEntityMgr.create(modelNote);
    }

    @Override
    public List<ModelNotes> getAllByModelSummaryId(String modelSummaryId) {
        ModelSummary summary1 = modelSummaryEntityMgr.getByModelId(modelSummaryId);
        return modelNotesEntityMgr.getAllByModelSummaryId(String.valueOf(summary1.getPid()));
    }

    @Override
    public void deleteByNoteId(String noteId) {
        ModelNotes note = modelNotesEntityMgr.findByNoteId(noteId);
        modelNotesEntityMgr.delete(note);

    }

    @Override
    public void updateByNoteId(String noteId, NoteParams noteParams) {
        ModelNotes note = modelNotesEntityMgr.findByNoteId(noteId);
        note.setNotesContents(noteParams.getContent());
        note.setLastModificationTimestamp((new Date()).getTime());
        note.setLastModifiedByUser(noteParams.getUserName());
        modelNotesEntityMgr.update(note);
    }

    @Override
    public void copyNotes(String fromModelSummaryId, String toModelSummaryId) {
        ModelSummary fromSummary = modelSummaryEntityMgr.getByModelId(fromModelSummaryId);
        List<ModelNotes> notes = modelNotesEntityMgr.getAllByModelSummaryId(String.valueOf(fromSummary.getPid()));
        ModelSummary toSummary = modelSummaryEntityMgr.getByModelId(toModelSummaryId);
        for(ModelNotes note : notes) {
            System.out.println("content" + note.getNotesContents());
            ModelNotes copyNote = new ModelNotes();
            copyNote.setCreatedByUser(note.getCreatedByUser());
            copyNote.setCreationTimestamp(note.getCreationTimestamp());
            copyNote.setLastModificationTimestamp(note.getLastModificationTimestamp());
            copyNote.setLastModifiedByUser(note.getLastModifiedByUser());
            copyNote.setNotesContents(note.getNotesContents());
            copyNote.setOrigin(note.getOrigin());
            copyNote.setParentModelId(fromModelSummaryId);
            copyNote.setModelSummary(toSummary);
            copyNote.setId(UUID.randomUUID().toString());
            modelNotesEntityMgr.create(copyNote);
        }

    }

}
