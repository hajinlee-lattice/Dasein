package com.latticeengines.spark.service.impl;

import java.util.Collections;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.spark.exposed.service.LivySessionService;
import com.latticeengines.spark.testframework.SparkFunctionalTestNGBase;

public class LivySessionServiceImplTestNG extends SparkFunctionalTestNGBase {

    @Inject
    private LivySessionService sessionService;

    private Integer sessionId;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupLivyHost();
    }

    @AfterClass(groups = "functional", alwaysRun = true)
    public void tearDown() {
        if (sessionId != null) {
            sessionService.stopSession(new LivySession(livyHost, sessionId));
        }
    }

    @Test(groups = "functional")
    public void testCrud() {
        LivySession session = sessionService.startSession(this.getClass().getSimpleName(), //
                Collections.emptyMap(), Collections.emptyMap());
        Assert.assertNotNull(session);
        Assert.assertNotNull(session.getSessionId());

        sessionId = session.getSessionId();

        LivySession retrieved = sessionService.getSession(new LivySession(livyHost, sessionId));
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getSessionId(), session.getSessionId());
        Assert.assertEquals(retrieved.getDriverLogUrl(), session.getDriverLogUrl());
        Assert.assertEquals(retrieved.getSparkUiUrl(), session.getSparkUiUrl());

        sessionService.stopSession(session);

        retrieved = sessionService.getSession(new LivySession(livyHost, sessionId));
        Assert.assertNull(retrieved);
    }

}
