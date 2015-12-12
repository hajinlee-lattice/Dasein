package com.latticeengines.security.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.GrantedRight;
import com.latticeengines.security.exposed.globalauth.GlobalSessionManagementService;



public class GASessionCacheUnitTestNG {

    private static final String VALID_TOKEN = "98144782-1c4f-46f6-88c5-0aeab2d385ef.B902799AF7FDE2B2";
    private static final String INVALID_TOKEN = "invalid_token.5ECEFD633175D1FF";
    private static final int cacheExpirationInSeconds = 3;

    private static MockGlobalSessionMgr mockSessionMgr = new MockGlobalSessionMgr();
    private static GASessionCache cache;

    @BeforeClass(groups = "unit")
    public void setup() {
        cache = new GASessionCache(mockSessionMgr, cacheExpirationInSeconds);
    }

    @BeforeMethod(groups = "unit")
    public void cleanup() {
        cache.removeAll();
        mockSessionMgr.logoutAll();
        mockSessionMgr.login(VALID_TOKEN);
    }

    @Test(groups = "unit")
    public void testLoadAndRetrieveValid() throws ExecutionException {
        Session session = cache.retrieve(VALID_TOKEN);
        Assert.assertNotNull(session, "Should get session from first invocation.");

        Assert.assertNotNull(session.getRights());
        Assert.assertEquals(session.getAccessLevel(), AccessLevel.INTERNAL_USER.name());

//        session = cache.retrieve(VALID_TOKEN);
//        Assert.assertNotNull(session, "Should get session from second invocation.");
//
//        Assert.assertNotNull(session.getRights());
//        Assert.assertEquals(session.getAccessLevel(), AccessLevel.INTERNAL_USER.name());

//        // wait to expire
//        try {
//            Thread.sleep(cacheExpirationInSeconds * 1000L + 500L);
//        } catch (InterruptedException e) {
//            Assert.fail("Failed to wait for the session to expire", e);
//        }
//        session = cache.retrieve(VALID_TOKEN);
//        Assert.assertNotNull(session, "Should get session after cache expired.");
    }

    @Test(groups = "unit", expectedExceptions = LedpException.class)
    public void testFirstLoadInvalid() throws ExecutionException {
        Session session = cache.retrieve(INVALID_TOKEN);
        Assert.assertNotNull(session, "Should get session from first invocation.");
    }

    @Test(groups = "unit", expectedExceptions = LedpException.class)
    public void testLoadValidThenExpire() throws ExecutionException {
        Session session = cache.retrieve(VALID_TOKEN);
        Assert.assertNotNull(session, "Should get session from first invocation.");

        mockSessionMgr.logout(VALID_TOKEN);
        cache.removeToken(VALID_TOKEN); // force go to GA
        cache.retrieve(VALID_TOKEN);
    }

    @Test(groups = "unit")
    public void testConcurrentRetrieve() throws InterruptedException, ExecutionException {
        List<Future<Integer>> futures = new ArrayList<>();
        int numTestCases = 20;
        ExecutorService executor = Executors.newFixedThreadPool(numTestCases);
        for (int i = 0; i < numTestCases; i++) {
            Future<Integer> future = executor.submit(new Callable<Integer>() {
                @Override
                public Integer call() throws InterruptedException {
                    // random initial delay
                    Thread.sleep(ThreadLocalRandom.current().nextInt(1, 500));
                    try {
                        test();
                        return 1;
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                        return 0;
                    }
                }

                private void test() {
                    String token = UUID.randomUUID() + ".81CACD38314FCB23";
                    try {
                        cache.retrieve(token);
                        Assert.fail("Retrieving a token before logging in should raises exception.");
                    } catch (LedpException e) {
                        Assert.assertEquals(e.getCode(), LedpCode.LEDP_18002);
                    }
                    mockSessionMgr.login(token);

                    // first retrieve
                    Session session = cache.retrieve(token);
                    Assert.assertNotNull(session);
                    Assert.assertNotNull(session.getRights());
                    Assert.assertEquals(session.getAccessLevel(), AccessLevel.INTERNAL_USER.name());

                    // second retrieve
                    session = cache.retrieve(token);
                    Assert.assertNotNull(session);
                    Assert.assertNotNull(session.getRights());
                    Assert.assertEquals(session.getAccessLevel(), AccessLevel.INTERNAL_USER.name());

                    // enforce to expire
                    mockSessionMgr.logout(token);
                    cache.removeToken(token);
                    try {
                        cache.retrieve(token);
                        Assert.fail("Retrieving an expired token should raises exception.");
                    } catch (LedpException e) {
                        Assert.assertEquals(e.getCode(), LedpCode.LEDP_18002);
                    }

                    // login again
                    mockSessionMgr.login(token);
                    session = cache.retrieve(token);
                    Assert.assertNotNull(session);

                    // logout
                    mockSessionMgr.logout(token);
                    cache.removeToken(token);
                    try {
                        cache.retrieve(token);
                        Assert.fail("Retrieving a logout session should raises exception.");
                    } catch (LedpException e) {
                        Assert.assertEquals(e.getCode(), LedpCode.LEDP_18002);
                    }

                    // login again
                    mockSessionMgr.login(token);
                    session = cache.retrieve(token);
                    Assert.assertNotNull(session);
                    mockSessionMgr.logout(token);
                }

            });
            futures.add(future);
        }

        int successCases = 0;
        for (Future<Integer> future: futures) {
            successCases += future.get();
        }

        Assert.assertEquals(successCases, numTestCases,
                String.format("Only %d out of %d test cases passed.", successCases, numTestCases));
    }

    private static class MockGlobalSessionMgr implements GlobalSessionManagementService {

        private Set<String> validTokens = new HashSet<>();

        @Override
        public Session retrieve(Ticket ticket) {
            if (validTokens.contains(ticket.getData())) {
                Session session =  new Session();
                session.setEmailAddress("xx@lattice-engines.com");
                Tenant tenant = new Tenant();
                tenant.setId("tenantID");
                session.setTenant(tenant);
                if (ThreadLocalRandom.current().nextBoolean()) {
                    List<GrantedRight> rights = AccessLevel.INTERNAL_ADMIN.getGrantedRights();
                    session.setRights(GrantedRight.getAuthorities(rights));
                } else {
                    session.setRights(Collections.singletonList(AccessLevel.INTERNAL_USER.name()));
                }
                return session;
            } else {
                throw new IllegalArgumentException("Invalid token");
            }
        }

        @Override
        public Session attach(Ticket ticket) {
            return null;
        }

        synchronized public void logout(String token) {
            validTokens.remove(token);
        }

        synchronized public void login(String token) {
            validTokens.add(token);
        }

        public void logoutAll() {
            validTokens.clear();
        }
    }

}
