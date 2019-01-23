package com.latticeengines.testframework.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.web.client.RestTemplate;

import com.amazonaws.services.sqs.model.UnsupportedOperationException;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.AuthorizationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.testframework.exposed.rest.LedpResponseErrorHandler;
import com.latticeengines.testframework.exposed.service.GlobalAuthTestBed;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public abstract class AbstractGlobalAuthTestBed implements GlobalAuthTestBed {

    private static Logger log = LoggerFactory.getLogger(AbstractGlobalAuthTestBed.class);

    private static final String customerBase = "/user/s-analytics/customers";

    private Map<String, UserDocument> userTenantSessions = new HashMap<>();

    protected List<Tenant> testTenants = new ArrayList<>();
    protected List<String> excludedCleanupTenantIds = new ArrayList<>();
    private Integer mainTenantIdx = 0;

    protected RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
    protected RestTemplate magicRestTemplate = HttpClientUtils.newRestTemplate();
    protected final AuthorizationHeaderHttpRequestInterceptor authHeaderInterceptor = new AuthorizationHeaderHttpRequestInterceptor(
            "");
    private LedpResponseErrorHandler errorHandler = new LedpResponseErrorHandler();

    private UserDocument currentUser;

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private RedshiftService redshiftService;

    @Inject
    private S3Service s3Service;

    @Value("${aws.customer.s3.bucket}")
    private String customerBucket;

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
    public LedpResponseErrorHandler getErrorHandler() {
        return errorHandler;
    }

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
        switchToSuperAdmin(getMainTestTenant());
    }

    @Override
    public void switchToInternalAdmin() {
        switchToInternalAdmin(getMainTestTenant());
    }

    @Override
    public void switchToInternalUser() {
        switchToInternalUser(getMainTestTenant());
    }

    @Override
    public void switchToExternalAdmin() {
        switchToExternalAdmin(getMainTestTenant());
    }

    @Override
    public void switchToExternalUser() {
        switchToExternalUser(getMainTestTenant());
    }

    @Override
    public void switchToThirdPartyUser() {
        switchToThirdPartyUser(getMainTestTenant());
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
    public void cleanupS3() {
        for (Tenant tenant : testTenants) {
            if (excludedCleanupTenantIds.contains(tenant.getId())) {
                log.info("Skip cleaning up S3 for" + tenant.getId());
            } else {
                log.info("Clean up S3 for test tenant " + tenant.getId());
                s3Service.cleanupPrefix(customerBucket, CustomerSpace.parse(tenant.getId()).getTenantId());
            }
        }
    }

    @Override
    public void cleanupDlZk() {
    }

    @Override
    public void cleanupPlsHdfs() {
        for (Map.Entry<String, UserDocument> entry : userTenantSessions.entrySet()) {
            log.info("Logging out token for " + entry.getKey());
            UserDocument userDoc = entry.getValue();
            if (userDoc != null) {
                try {
                    logout(userDoc);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }

        for (Tenant tenant : testTenants) {
            if (excludedCleanupTenantIds.contains(tenant.getId())) {
                log.info("Skip cleaning up " + tenant.getId());
            } else {
                log.info("Clean up test tenant " + tenant.getId());
                deleteTenant(tenant);
            }
        }

        cleanupHdfs();
    }

    @Override
    public void cleanupRedshift() {
        for (Tenant tenant : testTenants) {
            if (excludedCleanupTenantIds.contains(tenant.getId())) {
                log.info("Skip cleaning up redshift for" + tenant.getId());
            } else {
                log.info("Clean up redshift test tenant " + tenant.getId());
                List<String> tables = redshiftService.getTables(CustomerSpace.parse(tenant.getId()).getTenantId());
                tables.forEach(redshiftService::dropTable);
            }
        }
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
     * add an extra test tenant with given name:
     * tenantName.tenantName.Production
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
     * add an extra test tenant with given name:
     * tenantName.tenantName.Production
     */
    @Override
    public Tenant useExistingTenantAsMain(String tenantName) {
        String fullTenantId = CustomerSpace.parse(tenantName).toString();
        addExtraTestTenant(fullTenantId);
        Tenant tenant = new Tenant(fullTenantId);
        setMainTestTenant(tenant);
        for (AccessLevel accessLevel : AccessLevel.values()) {
            bootstrapUser(accessLevel, tenant);
        }
        if (excludedCleanupTenantIds == null) {
            excludedCleanupTenantIds = new ArrayList<>();
        }
        excludedCleanupTenantIds.add(fullTenantId);
        return tenant;
    }

    /**
     * Use an already logged in UserDocument
     * 
     * @param doc
     */
    public void useSessionDoc(UserDocument doc) {
        currentUser = doc;
        authHeaderInterceptor.setAuthValue(doc.getTicket().getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { authHeaderInterceptor }));
    }

    protected Tenant addBuiltInTestTenant() {
        String fullTenantId = TestFrameworkUtils.generateTenantName();
        return addTestTenant(fullTenantId);
    }

    protected Tenant addTestTenant(String fullTenantId) {
        String tenantId = CustomerSpace.parse(fullTenantId).toString();
        Tenant tenant = new Tenant();
        tenant.setId(tenantId);
        tenant.setName(fullTenantId);

        createTenant(tenant);
        Tenant retrievedTenant = getTenantBasedOnId(tenantId);
        testTenants.add(retrievedTenant);

        log.info("Adding test tenant " + tenantId);
        return tenant;
    }

    @Override
    public void excludeTestTenantsForCleanup(List<Tenant> tenants) {
        excludedCleanupTenantIds.addAll(tenants.stream().map(Tenant::getId).collect(Collectors.toList()));
    }

    public abstract void createTenant(Tenant tenant);

    public abstract void deleteTenant(Tenant tenant);

    public abstract Tenant getTenantBasedOnId(String tenantId);

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
        for (Tenant tenant : testTenants) {
            if (excludedCleanupTenantIds.contains(tenant.getId())) {
                log.info("Skip cleaning up HDFS for  " + tenant.getId());
            } else {
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
        return level.name() + "|" + tenant.getId();
    }

    public abstract UserDocument loginAndAttach(String username, String password, Tenant tenant);

    protected abstract void logout(UserDocument userDocument);

    protected abstract void bootstrapUser(AccessLevel accessLevel, Tenant tenant);

    public UserDocument getCurrentUser() {
        return currentUser;
    }

    @Override
    public FeatureFlagValueMap getFeatureFlags() {
        throw new UnsupportedOperationException("FeatureFlag retrieval is not yet implemented");
    }
}
