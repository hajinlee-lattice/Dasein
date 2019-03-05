package com.latticeengines.security.exposed.globalauth.zendesk.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.test.web.client.ExpectedCount;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.test.web.client.match.MockRestRequestMatchers;
import org.springframework.test.web.client.response.DefaultResponseCreator;
import org.springframework.test.web.client.response.MockRestResponseCreators;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.zendesk.ZendeskRole;
import com.latticeengines.domain.exposed.security.zendesk.ZendeskUser;
import com.latticeengines.security.exposed.globalauth.zendesk.ZendeskService;

public class ZendeskServiceImplUnitTestNG {
    private static Map<Class<? extends Throwable>, Boolean> RETRY_EXCEPTION_MAP = Collections.singletonMap(
            RestClientException.class, true);

    private static final String DOMAIN = "https://test.zendesk.com";
    private static final String USERNAME = "test@gmail.com";
    private static final String TOKEN = "random_api_token";

    private static final ZendeskUser USER = new ZendeskUser();

    static {
        USER.setId(1234L);
        USER.setEmail("test@lattice-engines.com");
        USER.setName("testuser");
        USER.setRole(ZendeskRole.END_USER);
    }

    private ZendeskService svc;
    private MockRestServiceServer server;
    private RestTemplate template;
    private RetryTemplate retryTemplate;

    @BeforeMethod(groups = "unit")
    public void setUp() {
        // default to no retry
        initService(new SimpleRetryPolicy(1, RETRY_EXCEPTION_MAP, true));
    }

    @Test(groups = "unit")
    public void createOrUpdateUser() {
        configureMockServer(url("/api/v2/users/create_or_update.json"), HttpMethod.POST, HttpStatus.OK, wrap(USER));

        ZendeskUser req = new ZendeskUser();
        req.setEmail(USER.getEmail());
        req.setName(USER.getName());

        checkResponse(svc.createOrUpdateUser(req));
        server.verify();
    }

    @Test(groups = "unit")
    public void updateUser() {
        configureMockServer(url("/api/v2/users/" + USER.getId() + ".json"), HttpMethod.PUT, HttpStatus.OK, wrap(USER));
        configureMockServer(url("/api/v2/users/-1.json"), HttpMethod.PUT, HttpStatus.NOT_FOUND, null);

        // happy path
        ZendeskUser req = new ZendeskUser();
        req.setId(USER.getId());
        checkResponse(svc.updateUser(req));

        // requested user does not exist
        ZendeskUser req2 = new ZendeskUser();
        req2.setId(-1L);
        checkLedpExceptionThrown(() -> svc.updateUser(req2), LedpCode.LEDP_19003);

        server.verify();
    }

    @Test(groups = "unit")
    public void findUserByEmail() {
        List<ZendeskUser> res = Collections.singletonList(USER);
        String path = url("/api/v2/users/search.json?query=email:" + USER.getEmail());
        configureMockServer(path, HttpMethod.GET, HttpStatus.OK, wrap(res));
        configureMockServer(path, HttpMethod.GET, HttpStatus.OK, wrap(Collections.emptyList()));

        // happy path
        ZendeskUser result = svc.findUserByEmail(USER.getEmail());
        checkResponse(result);

        // no user with specified email
        Assert.assertNull(svc.findUserByEmail(USER.getEmail()));

        server.verify();
    }

    @Test(groups = "unit")
    public void setUserPassword() {
        String path = url("/api/v2/users/" + USER.getId() + "/password.json");
        configureMockServer(path, HttpMethod.POST, HttpStatus.OK, wrap(USER));
        configureMockServer(path, HttpMethod.POST, HttpStatus.NOT_FOUND, null);

        // happy path
        svc.setUserPassword(USER.getId(), "password");

        // requested user does not exist
        checkLedpExceptionThrown(() -> svc.setUserPassword(USER.getId(), "password"), LedpCode.LEDP_19003);

        server.verify();
    }

    @Test(groups = "unit")
    public void testRateLimitingResponse() {
        server.expect(ExpectedCount.times(8), MockRestRequestMatchers.anything())
                .andRespond(MockRestResponseCreators.withStatus(HttpStatus.TOO_MANY_REQUESTS));

        // make sure each method handles rate limiting response correctly
        checkLedpExceptionThrown(() -> svc.createOrUpdateUser(USER), LedpCode.LEDP_00005);
        checkLedpExceptionThrown(() -> svc.setUserPassword(USER.getId(), "password"), LedpCode.LEDP_00005);
        checkLedpExceptionThrown(() -> svc.updateUser(USER), LedpCode.LEDP_00005);
        checkLedpExceptionThrown(() -> svc.findUserByEmail(USER.getEmail()), LedpCode.LEDP_00005);
        checkLedpExceptionThrown(() -> svc.suspendUser(USER.getId()), LedpCode.LEDP_00005);
        checkLedpExceptionThrown(() -> svc.unsuspendUser(USER.getId()), LedpCode.LEDP_00005);
        checkLedpExceptionThrown(() -> svc.suspendUserByEmail(USER.getEmail()), LedpCode.LEDP_00005);
        checkLedpExceptionThrown(() -> svc.unsuspendUserByEmail(USER.getEmail()), LedpCode.LEDP_00005);

        server.verify();
    }

    /**
     * Make sure that the service only retries on too many requests error (status code 429)
     * for client side errors (status code 4xx)
     */
    @Test(groups = "unit")
    public void retryOnlyOnRateLimitingError() {
        // retry one time
        initService(new SimpleRetryPolicy(2));

        server.expect(ExpectedCount.times(1), MockRestRequestMatchers.anything())
                .andRespond(MockRestResponseCreators.withStatus(HttpStatus.TOO_MANY_REQUESTS));
        server.expect(ExpectedCount.times(1), MockRestRequestMatchers.anything())
                .andRespond(MockRestResponseCreators.withStatus(HttpStatus.BAD_REQUEST));
        server.expect(ExpectedCount.times(1), MockRestRequestMatchers.anything())
                .andRespond(MockRestResponseCreators.withStatus(HttpStatus.TOO_MANY_REQUESTS));
        server.expect(ExpectedCount.times(1), MockRestRequestMatchers.anything())
                .andRespond(MockRestResponseCreators.withStatus(HttpStatus.NOT_FOUND));

        // retry on rate limiting error but fail on other 4xx errors
        checkLedpExceptionThrown(() -> svc.createOrUpdateUser(USER), LedpCode.LEDP_25037);
        checkLedpExceptionThrown(() -> svc.setUserPassword(USER.getId(), "password"), LedpCode.LEDP_19003);
        server.verify();
    }

    /**
     * Make sure the service retries on server side errors (status code 5xx)
     */
    @Test(groups = "unit")
    public void retryOnServerErrors() {
        // retry one time
        initService(new SimpleRetryPolicy(2));

        server.expect(ExpectedCount.times(1), MockRestRequestMatchers.anything())
                .andRespond(MockRestResponseCreators.withStatus(HttpStatus.SERVICE_UNAVAILABLE));
        server.expect(ExpectedCount.times(1), MockRestRequestMatchers.anything())
                .andRespond(MockRestResponseCreators.withStatus(HttpStatus.OK));
        server.expect(ExpectedCount.times(1), MockRestRequestMatchers.anything())
                .andRespond(MockRestResponseCreators.withStatus(HttpStatus.INTERNAL_SERVER_ERROR));
        server.expect(ExpectedCount.times(1), MockRestRequestMatchers.anything())
                .andRespond(MockRestResponseCreators.withStatus(HttpStatus.OK));

        svc.setUserPassword(USER.getId(), "password");
        svc.setUserPassword(USER.getId(), "password");
        server.verify();
    }

    /**
     * Make sure the service throws exception when retry limit is exceeded
     */
    @Test(groups = "unit")
    public void exhaustRetryLimits() {
        // retry one time
        initService(new SimpleRetryPolicy(2, RETRY_EXCEPTION_MAP, true));

        server.expect(ExpectedCount.times(2), MockRestRequestMatchers.anything())
                .andRespond(MockRestResponseCreators.withStatus(HttpStatus.TOO_MANY_REQUESTS));
        server.expect(ExpectedCount.times(2), MockRestRequestMatchers.anything())
                .andRespond(MockRestResponseCreators.withStatus(HttpStatus.INTERNAL_SERVER_ERROR));

        // should fail on the second attempt
        checkLedpExceptionThrown(() -> svc.createOrUpdateUser(USER), LedpCode.LEDP_00005);
        checkLedpExceptionThrown(() -> svc.createOrUpdateUser(USER), LedpCode.LEDP_00007);
        server.verify();
    }

    private void initService(RetryPolicy policy) {
        svc = new ZendeskServiceImpl(DOMAIN, USERNAME, TOKEN);
        try {
            template = (RestTemplate) FieldUtils.readField(svc, "template", true);
            retryTemplate = (RetryTemplate) FieldUtils.readField(svc, "retryTemplate", true);
            retryTemplate.setRetryPolicy(policy);
            retryTemplate.setThrowLastExceptionOnExhausted(true);
            server = MockRestServiceServer.createServer(template);
        } catch (IllegalAccessException e) {
            Assert.fail("Failed to initialize mock api server", e);
        }
    }

    private void checkLedpExceptionThrown(Runnable r, LedpCode code) {
        try {
            r.run();
            Assert.fail("Should thrown exception");
        } catch (Exception e) {
            checkLedpException(e, code);
        }
    }

    private void checkLedpException(Exception e, LedpCode code) {
        Assert.assertTrue(e instanceof LedpException);
        LedpException exception = (LedpException) e;
        Assert.assertEquals(exception.getCode(), code);
    }

    private void checkResponse(ZendeskUser response) {
        Assert.assertNotNull(response);
        Assert.assertEquals(response.getEmail(), USER.getEmail());
        Assert.assertEquals(response.getId(), USER.getId());
        Assert.assertEquals(response.getRole(), USER.getRole());
    }

    private Map<String, ZendeskUser> wrap(ZendeskUser user) {
        return Collections.singletonMap("user", user);
    }

    private Map<String, List<ZendeskUser>> wrap(List<ZendeskUser> users) {
        return Collections.singletonMap("users", users);
    }

    private String url(String path) {
        return DOMAIN + path;
    }

    private void configureMockServer(String expectedPath, HttpMethod method, HttpStatus status, Object obj) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            DefaultResponseCreator creator = MockRestResponseCreators
                    .withStatus(status)
                    .contentType(MediaType.APPLICATION_JSON_UTF8);
            if (obj != null) {
                creator = creator.body(mapper.writeValueAsString(obj));
            }
            server.expect(MockRestRequestMatchers.requestTo(expectedPath))
                    .andExpect(MockRestRequestMatchers.method(method))
                    .andRespond(creator);
        } catch (JsonProcessingException e) {
            Assert.fail("Failed to serialize object: " + obj);
        }
    }
}
