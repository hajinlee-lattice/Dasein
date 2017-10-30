package com.latticeengines.pls.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.NoteOrigin;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.RatingEngineEntityMgr;
import com.latticeengines.pls.entitymanager.impl.RatingEngineEntityMgrImplTestNG;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.RatingEngineNoteService;

public class RatingEngineNoteServiceImplTestNG extends PlsFunctionalTestNGBase {

    public static final String content1 = "content1";

    public static final String content2 = "content2";

    public static final String content3 = "content3";

    public static final String user1 = "user1";

    public static final String user2 = "user2";

    @Autowired
    private RatingEngineEntityMgrImplTestNG ratingEngineEntityMgrImplTestNG;

    @Autowired
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    @Autowired
    private RatingEngineNoteService ratingEngineNoteService;

    private Tenant tenant;

    private RatingEngine ratingEngine;

    private RatingEngineNote note1;

    private RatingEngineNote note2;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        ratingEngineEntityMgrImplTestNG.setup();
        tenant = ratingEngineEntityMgrImplTestNG.getTenant();
        ratingEngine = ratingEngineEntityMgr
                .createOrUpdateRatingEngine(ratingEngineEntityMgrImplTestNG.getRatingEngine(), tenant.getId());
    }

    @Test(groups = "functional")
    public void testCreate() {
        NoteParams noteParams = new NoteParams();
        noteParams.setContent(content1);
        noteParams.setUserName(user1);
        noteParams.setOrigin(NoteOrigin.NOTE.name());
        note1 = ratingEngineNoteService.create(ratingEngine.getId(), noteParams);
        checkRatingEngineNote(note1, noteParams, this.ratingEngine);

        noteParams = new NoteParams();
        noteParams.setContent(content2);
        noteParams.setUserName(user2);
        noteParams.setOrigin(NoteOrigin.NOTE.name());
        note2 = ratingEngineNoteService.create(ratingEngine.getId(), noteParams);
        checkRatingEngineNote(note2, noteParams, this.ratingEngine);
    }

    private void checkRatingEngineNote(RatingEngineNote note, NoteParams params, RatingEngine ratingEngine) {
        Assert.assertNotNull(note.getPid());
        Assert.assertNotNull(note.getId());
        Assert.assertEquals(note.getCreatedByUser(), params.getUserName());
        Assert.assertEquals(note.getOrigin(), NoteOrigin.NOTE.name());
        Assert.assertEquals(note.getRatingEngine().getId(), ratingEngine.getId());
        Assert.assertEquals(note.getNotesContents(), params.getContent());
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreate" })
    public void testGet() {
        List<RatingEngineNote> notes = ratingEngineNoteService.getAllByRatingEngineId(ratingEngine.getId());
        Assert.assertNotNull(notes);
        Assert.assertEquals(notes.size(), 2);
    }

    @Test(groups = "functional", dependsOnMethods = { "testGet" })
    public void testUpdate() {
        NoteParams noteParams = new NoteParams();
        noteParams.setContent(content3);
        noteParams.setUserName(user2);
        ratingEngineNoteService.updateById(note1.getId(), noteParams);

        RatingEngineNote updatedNote = ratingEngineNoteService.findById(note1.getId());
        Assert.assertNotNull(updatedNote);
        Assert.assertEquals(updatedNote.getNotesContents(), noteParams.getContent());
        Assert.assertEquals(updatedNote.getLastModifiedByUser(), noteParams.getUserName());
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpdate" })
    public void testDelete() {
        ratingEngineNoteService.deleteById(note1.getId());
        List<RatingEngineNote> notes = ratingEngineNoteService.getAllByRatingEngineId(ratingEngine.getId());
        Assert.assertNotNull(notes);
        Assert.assertEquals(notes.size(), 1);
        Assert.assertEquals(notes.get(0).getId(), note2.getId());
    }
}
