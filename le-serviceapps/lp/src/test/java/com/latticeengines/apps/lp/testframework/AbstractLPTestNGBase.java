package com.latticeengines.apps.lp.testframework;

import javax.inject.Inject;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Listeners;

import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-serviceapps-lp-context.xml" })
public abstract class AbstractLPTestNGBase extends AbstractTestNGSpringContextTests {

    @Inject
    protected OAuthUserEntityMgr userEntityMgr;

    protected void deleteOAuthUserIfExists(String userId) {
        if (userEntityMgr.get(userId) != null) {
            userEntityMgr.delete(userId);
        }
    }

}
