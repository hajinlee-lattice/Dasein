package com.latticeengines.pls.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.NoteOrigin;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class RatingEngineNoteResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String content1 = "content1";
    private static final String content3 = "content3";
    private static final String user1 = "user1";
    private static final String user2 = "user2";

    @Autowired
    private PlayResourceDeploymentTestNG playResourceDeploymentTestNG;

    private RatingEngine ratingEngine;

    private RatingEngineNote note1;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        playResourceDeploymentTestNG.setup();
        MultiTenantContext.setTenant(testBed.getMainTestTenant());
        ratingEngine = playResourceDeploymentTestNG.getRatingEngine();
    }

    @Test(groups = "deployment")
    public void testCreate() {
        NoteParams noteParams = new NoteParams();
        noteParams.setContent(content1);
        noteParams.setUserName(user1);
        noteParams.setOrigin(NoteOrigin.NOTE.name());
        Assert.assertTrue(restTemplate.postForObject(
                getRestAPIHostPort() + "/pls/ratingengines/" + ratingEngine.getId() + "/notes", noteParams,
                Boolean.class));
        List<?> listObject = restTemplate.getForObject(
                getRestAPIHostPort() + "/pls/ratingengines/" + ratingEngine.getId() + "/notes", List.class);
        List<RatingEngineNote> noteList = JsonUtils.convertList(listObject, RatingEngineNote.class);
        Assert.assertNotNull(noteList);
        Assert.assertEquals(noteList.size(), 1);
        note1 = noteList.get(0);
        checkRatingEngineNote(note1, noteParams, this.ratingEngine);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testCreate" })
    public void testUpdate() {
        NoteParams noteParams = new NoteParams();
        noteParams.setContent(content3);
        noteParams.setUserName(user2);
        Assert.assertTrue(restTemplate.postForObject(
                getRestAPIHostPort() + "/pls/ratingengines/" + ratingEngine.getId() + "/notes/" + note1.getId(),
                noteParams, Boolean.class));

        List<?> listObject = restTemplate.getForObject(
                getRestAPIHostPort() + "/pls/ratingengines/" + ratingEngine.getId() + "/notes", List.class);
        List<RatingEngineNote> noteList = JsonUtils.convertList(listObject, RatingEngineNote.class);
        Assert.assertNotNull(noteList);
        Assert.assertEquals(noteList.size(), 1);
        RatingEngineNote updatedNote = noteList.get(0);
        Assert.assertNotNull(updatedNote);
        Assert.assertEquals(updatedNote.getNotesContents(), noteParams.getContent());
        Assert.assertEquals(updatedNote.getLastModifiedByUser(), noteParams.getUserName());
    }

    @Test(groups = "deployment", dependsOnMethods = { "testUpdate" })
    public void testDelete() {
        List<?> listObject = restTemplate.getForObject(
                getRestAPIHostPort() + "/pls/ratingengines/" + ratingEngine.getId() + "/notes", List.class);
        List<RatingEngineNote> noteList = JsonUtils.convertList(listObject, RatingEngineNote.class);
        Assert.assertNotNull(noteList);
        Assert.assertEquals(noteList.size(), 1);
        restTemplate.delete(
                getRestAPIHostPort() + "/pls/ratingengines/" + ratingEngine.getId() + "/notes/" + note1.getId());
        listObject = restTemplate.getForObject(
                getRestAPIHostPort() + "/pls/ratingengines/" + ratingEngine.getId() + "/notes", List.class);
        noteList = JsonUtils.convertList(listObject, RatingEngineNote.class);
        Assert.assertNotNull(noteList);
        for (RatingEngineNote note : noteList) {
            System.out.println("note is " + note);
        }
        Assert.assertEquals(noteList.size(), 0);
    }

    private void checkRatingEngineNote(RatingEngineNote note, NoteParams params, RatingEngine ratingEngine) {
        Assert.assertNotNull(note.getId());
        Assert.assertEquals(note.getCreatedByUser(), params.getUserName());
        Assert.assertEquals(note.getOrigin(), NoteOrigin.NOTE.name());
        Assert.assertEquals(note.getNotesContents(), params.getContent());
    }

}
