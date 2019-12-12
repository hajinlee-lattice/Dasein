package com.latticeengines.apps.lp.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import javax.inject.Inject;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.lp.entitymgr.SourceFileEntityMgr;
import com.latticeengines.apps.lp.testframework.LPFunctionalTestNGBase;
import com.latticeengines.domain.exposed.pls.SourceFile;

public class SourceFileEntityMgrImplTestNG extends LPFunctionalTestNGBase {

    @Inject
    private SourceFileEntityMgr sourceFileEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "functional")
    public void testGetApplicationId() {
        String name = "SomeFileForApplicationId";
        String applicationId = "applicationId_00010";
        String path = "path";
        SourceFile sourceFile = new SourceFile();
        sourceFile.setName(name);
        sourceFile.setPath(path);
        sourceFile.setApplicationId(applicationId);
        sourceFileEntityMgr.create(sourceFile);

        SourceFile sourceFile2 = new SourceFile();
        sourceFile2.setName(name + "2");
        sourceFile2.setPath(path + "2");
        sourceFile2.setApplicationId(applicationId + "2");
        sourceFileEntityMgr.create(sourceFile2);

        SourceFile sourceFile3 = sourceFileEntityMgr.findByApplicationId(applicationId);
        assertEquals(sourceFile.getName(), sourceFile3.getName());
        assertEquals(sourceFile.getPath(), sourceFile3.getPath());
        assertEquals(sourceFile.getPid(), sourceFile3.getPid());
    }

    @Test(groups = "functional")
    public void testGetWorkflowPid() {
        String name = "SomeFileForWorkflowPid";
        Long workflowPid1 = Long.valueOf(111);
        String path = "path";
        SourceFile sourceFile = new SourceFile();
        sourceFile.setName(name);
        sourceFile.setPath(path);
        sourceFile.setWorkflowPid(workflowPid1);
        sourceFileEntityMgr.create(sourceFile);

        SourceFile sourceFile2 = sourceFileEntityMgr.findByWorkflowPid(workflowPid1);
        assertEquals(sourceFile.getName(), sourceFile2.getName());
        assertEquals(sourceFile.getPath(), sourceFile2.getPath());
        assertEquals(sourceFile.getPid(), sourceFile2.getPid());
    }
}
