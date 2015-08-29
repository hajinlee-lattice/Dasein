package com.latticeengines.pls.traffictest;

import static org.testng.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.hadoop.conf.Configuration;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.pls.KeyValue;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.pls.entitymanager.KeyValueEntityMgr;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.entitymanager.TenantEntityMgr;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.impl.ModelSummaryParser;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.UserService;

public class TrafficLoadTestNG extends PlsDeploymentTestNGBase {

    private static Log log = LogFactory.getLog(TrafficLoadTestNG.class);

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private KeyValueEntityMgr keyValueEntityMgr;

    @Autowired
    private ModelSummaryParser modelSummaryParser;

    @Autowired
    private UserService userService;

    private int numOfThreads;

    private int numOfTenants;

    private int numOfUsers;

    private int numOfRuns;

    private int threadSleep;

    private static ExecutorService executor;

    private List<Tenant> tenantList = new ArrayList<>();

    private Map<Tenant, List<User>> users = new HashMap<>();

    private static final String password = "EETAlfvFzCdm6/t3Ro8g89vzZo6EDCbucJMTPhYgWiE=";

    @Parameters({ "numOfThreads", "numOfTenants", "numOfUsers", "numOfRuns", "threadSleep" })
    @BeforeClass(groups = { "load" })
    public void setup(String numOfThreads, String numOfTenants, String numOfUsers, String numOfRuns, String threadSleep)
            throws Exception {
        setupTestEnvironment();

        this.numOfThreads = Integer.parseInt(numOfThreads);
        this.numOfTenants = Integer.parseInt(numOfTenants);
        this.numOfUsers = Integer.parseInt(numOfUsers);
        this.numOfRuns = Integer.parseInt(numOfRuns);
        this.threadSleep = Integer.parseInt(threadSleep);
        executor = Executors.newFixedThreadPool(this.numOfThreads);
        createTenants();
        createUsers();
        Thread.sleep(this.threadSleep * 1000L);
    }

    @AfterClass(groups = { "load" })
    public void destroy() {
        for (Tenant tenant : tenantList) {
            for (User user : users.get(tenant)) {
                deleteUserByRestCall(user.getUsername());
            }
            try {
                String tenantId = tenant.getId();
                Tenant existingTenant = tenantEntityMgr.findByTenantId(tenantId);
                if (existingTenant != null) {
                    for (KeyValue keyValue : keyValueEntityMgr.findByTenantId(existingTenant.getPid())) {
                        keyValueEntityMgr.delete(keyValue);
                    }
                    ModelSummary modelSummary = modelSummaryEntityMgr.getByModelId(tenantId);
                    if (modelSummary != null) {
                        modelSummaryEntityMgr.delete(modelSummary);
                    }
                    tenantEntityMgr.delete(existingTenant);
                }
                deleteTenantByRestCall(tenant.getId());
            } catch (Exception e) {
            }
        }
    }

    private void createTenants() throws Exception {
        for (int i = 0; i < numOfTenants; i++) {
            String tenantId = "T" + i;
            Tenant tenant = new Tenant();
            tenant.setId(tenantId);
            tenant.setName("T" + i);
            deleteTenantByRestCall(tenant.getId());
            createTenantByRestCall(tenant);
            tenantList.add(tenant);
            createModel(tenant);
        }
    }

    @SuppressWarnings("unchecked")
    private void createModel(Tenant tenant) throws Exception {
        String dir = modelingServiceHdfsBaseDir + "/" + tenant.getName() + "/models/Q_PLS_Modeling_" + tenant.getName()
                + "/8e3a9d8c-3bc1-4d21-9c91-0af28afc5c9a/1423547416066_0001/";
        InputStream modelSummaryFileAsStream = ClassLoader
                .getSystemResourceAsStream("com/latticeengines/pls/functionalframework/modelsummary-marketo.json");
        String contents = new String(IOUtils.toByteArray(modelSummaryFileAsStream));
        ModelSummary summary = modelSummaryParser.parse("", contents);
        KeyValue keyValue = summary.getDetails();
        JSONParser jsonParser = new JSONParser();
        JSONObject modelSummary = (JSONObject) jsonParser.parse(keyValue.getPayload());
        JSONObject modelDetails = (JSONObject) jsonParser.parse(modelSummary.get("ModelDetails").toString());
        modelDetails.put("Name", tenant.getName());
        modelSummary.put("ModelDetails", modelDetails);
        HdfsUtils.rmdir(yarnConfiguration, dir);
        HdfsUtils.mkdir(yarnConfiguration, dir);
        HdfsUtils.mkdir(yarnConfiguration, dir + "/enhancements");
        HdfsUtils.writeToFile(yarnConfiguration, dir + "/enhancements/modelsummary.json", modelSummary.toJSONString());
        HdfsUtils.writeToFile(yarnConfiguration, dir + "/test_model.csv", modelSummary.toJSONString());
        HdfsUtils.writeToFile(yarnConfiguration, dir + "/test_readoutsample.csv", modelSummary.toJSONString());
        HdfsUtils.writeToFile(yarnConfiguration, dir + "/test_scored.txt", modelSummary.toJSONString());
        HdfsUtils.writeToFile(yarnConfiguration, dir + "/test_explorer.csv", modelSummary.toJSONString());
        HdfsUtils.writeToFile(yarnConfiguration, dir + "/rf_model.txt", modelSummary.toJSONString());
    }

    private void createUsers() throws Exception {
        for (Tenant tenant : tenantList) {
            users.put(tenant, new ArrayList<User>());
            for (int i = 0; i < numOfUsers; i++) {
                User user = new User();
                user.setFirstName("test");
                user.setLastName("test");
                user.setEmail(tenant.getName() + "myemail" + i);
                Credentials userCreds = new Credentials();
                userCreds.setUsername(tenant.getName() + "_testuser_" + i);
                user.setUsername(userCreds.getUsername());
                userCreds.setPassword(password);
                deleteUserByRestCall(user.getUsername());

                UserRegistration userReg = new UserRegistration();
                userReg.setUser(user);
                userReg.setCredentials(userCreds);

                userService.createUser(userReg);
                users.get(tenant).add(user);
                grantRights(tenant, user);
            }
        }
    }

    private void grantRights(Tenant tenant, User user) {
        userService.assignAccessLevel(AccessLevel.SUPER_ADMIN, tenant.getId(), user.getUsername());
    }

    @Test(groups = "load", enabled = true)
    public void testWorkFlow() throws InterruptedException, ExecutionException {
        for (int i = 0; i < numOfRuns; i++) {
            List<Future<List<Long>>> futures = new ArrayList<>();
            for (final Tenant tenant : tenantList) {
                for (int j = 0; j < numOfUsers; j++) {
                    final int userNum = j;
                    Future<List<Long>> future = executor.submit(new Callable<List<Long>>() {

                        private RestTemplate restTemplate = new RestTemplate();
                        private AuthorizationHeaderHttpRequestInterceptor addAuthHeader = new AuthorizationHeaderHttpRequestInterceptor(
                                "");
                        private TimeStamp startTime;
                        private TimeStamp finishTime;
                        private UserDocument userDoc;
                        private ModelSummary modelSummary;
                        @SuppressWarnings("rawtypes")
                        private List response;

                        @Override
                        public List<Long> call() throws Exception {
                            Random random = new Random();
                            List<Long> timeConsumptions = new ArrayList<>();
                            restTemplate.setErrorHandler(new ThrowExceptionResponseErrorHandler());

                            timeConsumptions.add(loginMainPage());
                            Thread.sleep(random.nextInt(10) * 500L);

                            timeConsumptions.add(loginAndAttach(tenant, tenant.getName() + "_testuser_" + userNum));
                            Thread.sleep(random.nextInt(10) * 500L);

                            timeConsumptions.add(getModelSummaries());
                            Thread.sleep(random.nextInt(3) * 500L);

                            timeConsumptions.add(getModelSummary());
                            Thread.sleep(random.nextInt(10) * 500L);

                            timeConsumptions.add(logOut());
                            return timeConsumptions;
                        }

                        private Long loginMainPage() {
                            startTime = TimeStamp.getCurrentTime();
                            restTemplate.getForObject(getRestAPIHostPort(), String.class);
                            finishTime = TimeStamp.getCurrentTime();
                            return finishTime.getSeconds() - startTime.getSeconds();
                        }

                        private Long loginAndAttach(Tenant tenant, String username) {
                            Credentials creds = new Credentials();
                            creds.setUsername(username);
                            creds.setPassword(DigestUtils.sha256Hex("admin"));
                            startTime = TimeStamp.getCurrentTime();
                            LoginDocument doc = restTemplate.postForObject(getRestAPIHostPort() + "/pls/login", creds,
                                    LoginDocument.class, new Object[] {});
                            addAuthHeader.setAuthValue(doc.getData());
                            restTemplate.setInterceptors(Arrays
                                    .asList(new ClientHttpRequestInterceptor[] { addAuthHeader }));
                            userDoc = restTemplate.postForObject(getRestAPIHostPort() + "/pls/attach", tenant,
                                    UserDocument.class, new Object[] {});
                            finishTime = TimeStamp.getCurrentTime();
                            assertTrue(userDoc.isSuccess());
                            return finishTime.getSeconds() - startTime.getSeconds();
                        }

                        private Long getModelSummaries() {
                            startTime = TimeStamp.getCurrentTime();
                            response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/",
                                    List.class);
                            finishTime = TimeStamp.getCurrentTime();
                            assertNotNull(response);
                            assertEquals(response.size(), 1);
                            return finishTime.getSeconds() - startTime.getSeconds();
                        }

                        private Long getModelSummary() {
                            @SuppressWarnings({ "unchecked", "rawtypes" })
                            Map<String, String> map = (Map) response.get(0);
                            startTime = TimeStamp.getCurrentTime();
                            modelSummary = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/"
                                    + map.get("Id"), ModelSummary.class);
                            finishTime = TimeStamp.getCurrentTime();
                            assertTrue(modelSummary.getName().startsWith(tenant.getName()));
                            assertNotNull(modelSummary.getDetails());
                            return finishTime.getSeconds() - startTime.getSeconds();
                        }

                        private Long logOut() {
                            startTime = TimeStamp.getCurrentTime();
                            userDoc = restTemplate.getForObject(getRestAPIHostPort() + "/pls/users/logout",
                                    UserDocument.class);
                            finishTime = TimeStamp.getCurrentTime();
                            assertTrue(userDoc.isSuccess());
                            return finishTime.getSeconds() - startTime.getSeconds();
                        }
                    });
                    futures.add(future);
                }
            }

            Long loginMainPageTime = 0L;
            Long loginAndAttachTime = 0L;
            Long modelSummariesTime = 0L;
            Long modelSummaryTime = 0L;
            Long logOutTime = 0L;

            for (Future<List<Long>> future : futures) {
                List<Long> timeConsumptions = future.get();
                loginMainPageTime += timeConsumptions.get(0);
                loginAndAttachTime += timeConsumptions.get(1);
                modelSummariesTime += timeConsumptions.get(2);
                modelSummaryTime += timeConsumptions.get(3);
                logOutTime += timeConsumptions.get(4);
            }
            int userSize = numOfTenants * numOfUsers;
            System.out
                    .println(String.format("Log in to main page: %f seconds", Math.ceil(loginMainPageTime / userSize)));
            System.out.println(String.format("Log and Attach: %f seconds", Math.ceil(loginAndAttachTime / userSize)));
            System.out.println(String.format("Get Model Summaries: %f seconds",
                    Math.ceil(modelSummariesTime / userSize)));
            System.out.println(String.format("Get Model Summary: %f seconds", Math.ceil(modelSummaryTime / userSize)));
            System.out.println(String.format("Log out: %f seconds", Math.ceil(logOutTime / userSize)));
        }
    }

    class ThrowExceptionResponseErrorHandler implements ResponseErrorHandler {

        @Override
        public boolean hasError(ClientHttpResponse response) throws IOException {
            return response.getStatusCode() != HttpStatus.OK;
        }

        @Override
        public void handleError(ClientHttpResponse response) throws IOException {

            String responseBody = IOUtils.toString(response.getBody());

            log.info("Error response from rest call: " + response.getStatusCode() + " " + response.getStatusText()
                    + " " + responseBody);
            throw new RuntimeException(responseBody);
        }
    }
}
