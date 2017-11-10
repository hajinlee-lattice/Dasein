package com.latticeengines.apps.cdl.service.impl;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.RatingEngineEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.RatingEngineNoteEntityMgr;
import com.latticeengines.apps.cdl.service.RatingEngineNoteService;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;

@Component("ratingEngineNoteService")
public class RatingEngineNoteServiceImpl implements RatingEngineNoteService {

    @Autowired
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    @Autowired
    private RatingEngineNoteEntityMgr ratingEngineNoteEntityMgr;

    @Override
    public RatingEngineNote findById(String id) {
        return ratingEngineNoteEntityMgr.findById(id);
    }

    @Override
    public RatingEngineNote create(String ratingEngineId, NoteParams noteParams) {
        RatingEngineNote ratingEngineNote = new RatingEngineNote();
        ratingEngineNote.setCreatedByUser(noteParams.getUserName());
        ratingEngineNote.setLastModifiedByUser(noteParams.getUserName());

        Long nowTimestamp = (new Date()).getTime();
        ratingEngineNote.setCreationTimestamp(nowTimestamp);
        ratingEngineNote.setLastModificationTimestamp(nowTimestamp);
        ratingEngineNote.setNotesContents(noteParams.getContent());
        RatingEngine ratingEngine = ratingEngineEntityMgr.findById(ratingEngineId);
        ratingEngineNote.setRatingEngine(ratingEngine);
        ratingEngineNote.setOrigin(noteParams.getOrigin());
        ratingEngineNote.setId(UUID.randomUUID().toString());
        ratingEngineNoteEntityMgr.create(ratingEngineNote);
        return ratingEngineNote;
    }

    @Override
    public void deleteById(String id) {
        RatingEngineNote note = ratingEngineNoteEntityMgr.findById(id);
        ratingEngineNoteEntityMgr.delete(note);
    }

    @Override
    public void updateById(String id, NoteParams noteParams) {
        RatingEngineNote note = ratingEngineNoteEntityMgr.findById(id);
        note.setNotesContents(noteParams.getContent());
        note.setLastModificationTimestamp((new Date()).getTime());
        note.setLastModifiedByUser(noteParams.getUserName());
        ratingEngineNoteEntityMgr.update(note);
    }

    @Override
    public List<RatingEngineNote> getAllByRatingEngineId(String ratingEngineId) {
        RatingEngine ratingEngine = ratingEngineEntityMgr.findById(ratingEngineId);
        return ratingEngineNoteEntityMgr.getAllByRatingEngine(ratingEngine);
    }

}
