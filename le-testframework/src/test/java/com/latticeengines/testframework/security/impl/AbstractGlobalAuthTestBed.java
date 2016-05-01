package com.latticeengines.testframework.security.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.AuthorizationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;
import com.latticeengines.testframework.rest.LedpResponseErrorHandler;
import com.latticeengines.testframework.security.GlobalAuthTestBed;

public abstract class AbstractGlobalAuthTestBed implements GlobalAuthTestBed {

    private static Log log = LogFactory.getLog(AbstractGlobalAuthTestBed.class);

    private static final String customerBase = "/user/s-analytics/customers";

    private Map<String, UserDocument> userTenantSessions = new HashMap<>();

    protected List<Tenant> testTenants = new ArrayList<>();
    private Integer mainTenantIdx = 0;

    protected RestTemplate restTemplate = new RestTemplate();
    protected RestTemplate magicRestTemplate = new RestTemplate();
    protected AuthorizationHeaderHttpRequestInterceptor authHeaderInterceptor = new AuthorizationHeaderHttpRequestInterceptor(
            "");
    private LedpResponseErrorHandler errorHandler = new LedpResponseErrorHandler();

    @Autowired
    private ApplicationContext applicationContext;

    @PostConstruct
    private void postConstruct() {
        MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor(
                Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        magicRestTemplate.setErrorHandler(errorHandler);

        restTemplate.setErrorHandler(errorHandler);
    }

    @Override
    public RestTemplate getMagicRestTemplate() {
        return magicRestTemplate;
    }

    @Override
    public RestTemplate getRestTemplate() {
        return restTemplate;
    }

    @Override
    public LedpResponseErrorHandler getErrorHandler() { return errorHandler; }

    @Override
    public List<Tenant> getTestTenants() {
        return testTenants;
    }

    @Override
    public void setMainTestTenant(Tenant tenant) {
        for (int i = 0; i < testTenants.size(); i++) {
            if (tenant.getId().equals(testTenants.get(i).getId())) {
                mainTenantIdx = i;
                return;
            }
        }
        throw new RuntimeException("Tenant " + tenant.getId() + " is not registered as a test tenant");
    }

    @Override
    public Tenant getMainTestTenant() {
        return getTestTenants().get(mainTenantIdx);
    }

    @Override
    public void switchToSuperAdmin() {
        switchToSuperAdmin(getTestTenants().get(mainTenantIdx));
    }

    @Override
    public void switchToInternalAdmin() {
        switchToInternalAdmin(getTestTenants().get(mainTenantIdx));
    }

    @Override
    public void switchToInternalUser() {
        switchToInternalUser(getTestTenants().get(mainTenantIdx));
    }

    @Override
    public void switchToExternalAdmin() {
        switchToExternalAdmin(getTestTenants().get(mainTenantIdx));
    }

    @Override
    public void switchToExternalUser() {
        switchToExternalUser(getTestTenants().get(mainTenantIdx));
    }

    @Override
    public void switchToThirdPartyUser() {
        switchToThirdPartyUser(getTestTenants().get(mainTenantIdx));
    }

    @Override
    public void switchToSuperAdmin(Tenant tenant) {
        switchToTheSessionWithAccessLevel(AccessLevel.SUPER_ADMIN, tenant);
    }

    @Override
    public void switchToInternalAdmin(Tenant tenant) {
        switchToTheSessionWithAccessLevel(AccessLevel.INTERNAL_ADMIN, tenant);
    }

    @Override
    public void switchToInternalUser(Tenant tenant) {
        switchToTheSessionWithAccessLevel(AccessLevel.INTERNAL_USER, tenant);
    }

    @Override
    public void switchToExternalAdmin(Tenant tenant) {
        switchToTheSessionWithAccessLevel(AccessLevel.EXTERNAL_ADMIN, tenant);
    }

    @Override
    public void switchToExternalUser(Tenant tenant) {
        switchToTheSessionWithAccessLevel(AccessLevel.EXTERNAL_USER, tenant);
    }

    @Override
    public void switchToThirdPartyUser(Tenant tenant) {
        switchToTheSessionWithAccessLevel(AccessLevel.THIRD_PARTY_USER, tenant);
    }

    @Override
    public void cleanupDlZk() {
    }

    @Override
    public void cleanupPlsHdfs() {
        for (Map.Entry<String, UserDocument> entry: userTenantSessions.entrySet()) {
            log.info("Logging out token for " + entry.getKey());
            UserDocument userDoc = entry.getValue();
            if (userDoc != null) {
                try {
                    logout(userDoc);
                } catch (Exception e) {
                    log.error(e);
                }
            }
        }

        for (Tenant tenant: testTenants) {
            log.info("Clean up test tenant " + tenant.getId());
            deleteTenant(tenant);
        }

        cleanupHdfs();
    }

    /**
     * bootstrap with multiple GA tenant
     */
    @Override
    public void bootstrap(Integer numTenants) {
        for (int i = 0; i < numTenants; i++) {
            addBuiltInTestTenant();
        }
    }

    /**
     * add an extra test tenant with given name: tenantName.tenantName.Production
     */
    @Override
    public Tenant addExtraTestTenant(String tenantName) {
        String fullTenantId = CustomerSpace.parse(tenantName).toString();
        Tenant tenant = addTestTenant(fullTenantId);
        for (AccessLevel accessLevel : AccessLevel.values()) {
            bootstrapUser(accessLevel, tenant);
        }
        return tenant;
    }

    /**
     * Use an already logged in UserDocument
     * 
     * @param doc
     */
    public void useSessionDoc(UserDocument doc) {
        authHeaderInterceptor.setAuthValue(doc.getTicket().getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { authHeaderInterceptor }));
    }

    protected Tenant addBuiltInTestTenant() {
        String fullTenantId = TestFrameworkUtils.TENANTID_PREFIX + String.valueOf(System.currentTimeMillis());
        return addTestTenant(fullTenantId);
    }

    protected Tenant addTestTenant(String fullTenantId) {
        String tenantId = CustomerSpace.parse(fullTenantId).toString();
        Tenant tenant = new Tenant();
        tenant.setId(tenantId);
        tenant.setName(fullTenantId);

        createTenant(tenant);
        testTenants.add(tenant);

        log.info("Adding test tenant " + tenantId);
        return tenant;
    }

    public abstract void createTenant(Tenant tenant);

    public abstract void deleteTenant(Tenant tenant);

    private void cleanupHdfs() {
        Configuration yarnConfiguration;
        String podId;
        try {
            yarnConfiguration = (Configuration) applicationContext.getBean("yarnConfiguration");
            podId = PropertyUtils.getProperty("dataplatform.zk.pod.id");
            if (StringUtils.isEmpty(podId)) {
                podId = PropertyUtils.getProperty("camille.zk.pod.id");
            }
            if (StringUtils.isEmpty(podId)) {
                throw new RuntimeException("Cannot find pod id from context");
            }
        } catch (Exception e) {
            return;
        }
        for (Tenant tenant: testTenants) {
            String contractId = CustomerSpace.parse(tenant.getId()).getContractId();
            log.info("Clean up contract in HDFS: " + contractId);
            String customerSpace = CustomerSpace.parse(contractId).toString();
            String contractPath = PathBuilder.buildContractPath(podId, contractId).toString();
            try {
                if (HdfsUtils.fileExists(yarnConfiguration, contractPath)) {
                    HdfsUtils.rmdir(yarnConfiguration, contractPath);
                }
            } catch (Exception e) {
                log.warn("Failed to clean up " + contractPath);
            }

            String customerPath = new Path(customerBase).append(customerSpace).toString();
            try {
                if (HdfsUtils.fileExists(yarnConfiguration, customerPath)) {
                    HdfsUtils.rmdir(yarnConfiguration, customerPath);
                }
            } catch (Exception e) {
                log.warn("Failed to clean up " + customerPath);
            }

            contractPath = new Path(customerBase).append(contractId).toString();
            try {
                if (HdfsUtils.fileExists(yarnConfiguration, contractPath)) {
                    HdfsUtils.rmdir(yarnConfiguration, contractPath);
                }
            } catch (Exception e) {
                log.warn("Failed to clean up " + customerPath);
            }
        }
    }

    private void switchToTheSessionWithAccessLevel(AccessLevel level, Tenant tenant) {
        for (int i = 0; i < testTenants.size(); i++) {
            if (tenant.getId().equals(testTenants.get(i).getId())) {
                switchToTheSessionWithAccessLevel(level, i);
                return;
            }
        }
        throw new RuntimeException("Tenant " + tenant.getId() + " is not registered as a test tenant");
    }

    private void switchToTheSessionWithAccessLevel(AccessLevel level, Integer tenantIdx) {
        Tenant tenant = testTenants.get(tenantIdx);
        String key = getCacheKey(level, tenant);
        if (!userTenantSessions.containsKey(key)) {
            bootstrapUser(level, tenant);
            log.info("Login " + level + " user to the testing tenant " + tenant.getId());
            String username = TestFrameworkUtils.usernameForAccessLevel(level);
            String password = TestFrameworkUtils.GENERAL_PASSWORD;
            UserDocument uDoc = loginAndAttach(username, password, tenant);
            userTenantSessions.put(key, uDoc);
        }
        UserDocument uDoc = userTenantSessions.get(key);
        if (uDoc == null) {
            throw new NullPointerException("Could not find the session with access level " + level.name());
        }
        useSessionDoc(uDoc);
    }

    private String getCacheKey(AccessLevel level, Tenant tenant) {
        return level.name() + "|" +  tenant.getId();
    }

    public abstract UserDocument loginAndAttach(String username, String password, Tenant tenant);
    protected abstract void logout(UserDocument userDocument);
    protected abstract void bootstrapUser(AccessLevel accessLevel, Tenant tenant);

}
