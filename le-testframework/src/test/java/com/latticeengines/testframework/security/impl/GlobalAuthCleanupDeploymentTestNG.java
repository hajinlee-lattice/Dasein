package com.latticeengines.testframework.security.impl;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.documentdb.annotation.TenantIdColumn;
import com.latticeengines.documentdb.entity.BaseMultiTenantDocEntity;
import com.latticeengines.domain.exposed.admin.DeleteVisiDBDLRequest;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.dataloader.InstallResult;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;
import com.latticeengines.redshiftdb.exposed.utils.RedshiftUtils;
import com.latticeengines.remote.exposed.service.DataLoaderService;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.testframework.exposed.proxy.admin.AdminTenantProxy;
import com.latticeengines.testframework.exposed.rest.LedpResponseErrorHandler;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

@SuppressWarnings("deprecation")
@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-testframework-cleanup-context.xml" })
public class GlobalAuthCleanupDeploymentTestNG extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(GlobalAuthCleanupTestNG.class);
    private static final Long cleanupThreshold = TimeUnit.DAYS.toMillis(7);
    private static final String customerBase = "/user/s-analytics/customers";

    @Inject
    private TenantService tenantService;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private DataLoaderService dataLoaderService;

    @Inject
    private RedshiftService redshiftService;

    @Inject
    private S3Service s3Service;

    @Inject
    private AdminTenantProxy adminTenantProxy;

    @Resource(name = "docJdbcTemplate")
    private JdbcTemplate docJdbcTemplate;

    @Value("${aws.customer.s3.bucket}")
    private String customerBucket;

    @Value("${admin.test.deployment.api:http://localhost:9085}")
    private String adminApiHostPort;

    private Map<String, String> multiTenantDocStores;
    private Camille camille;
    private String podId;
    private RestTemplate magicRestTemplate = HttpClientUtils.newRestTemplate();
    private LedpResponseErrorHandler errorHandler = new LedpResponseErrorHandler();
    private List<String> errorTenants = new ArrayList<>();

    @BeforeClass(groups = "deployment")
    public void setup() {
        camille = CamilleEnvironment.getCamille();
        podId = CamilleEnvironment.getPodId();
        if (adminApiHostPort.endsWith("/")) {
            adminApiHostPort = adminApiHostPort.substring(0, adminApiHostPort.lastIndexOf("/"));
        }
        MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor(
                Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        magicRestTemplate.setErrorHandler(errorHandler);
        multiTenantDocStores = findAllMultiTenantDocStores();
    }

    @Test(groups = "deployment")
    public void cleanupErrorTenants() throws Exception {
        adminTenantProxy.login(TestFrameworkUtils.AD_USERNAME, TestFrameworkUtils.AD_PASSWORD);
        List<Tenant> tenants = tenantService.getAllTenants();
        log.info("Scanning through " + tenants.size() + " tenants ...");
        for (Tenant tenant : tenants) {
            TenantDocument tenantDoc = adminTenantProxy.getTenant(CustomerSpace.parse(tenant.getId()).getTenantId());
            if (tenantDoc.getBootstrapState().state == BootstrapState.State.ERROR
                    && (System.currentTimeMillis() - tenant.getRegisteredTime()) > cleanupThreshold) {
                log.info("Found a error tenant to clean up: " + tenant.getId());
                errorTenants.add(CustomerSpace.parse(tenant.getId()).getTenantId());

                cleanupTenantInGA(tenant);
                cleanupTenantInZK(CustomerSpace.parse(tenant.getId()).getContractId());
                cleanupTenantInDL(CustomerSpace.parse(tenant.getId()).getContractId());
                cleanupTenantInDocumentStores(tenant);
                deleteKey(tenant);
            }
        }

        cleanupRedshift();
        cleanupS3();
        cleanupTenantsInDocumentStores();
        cleanupTenantsInHdfs();
        cleanupZK();

        log.info("Finished cleaning up error tenants.");
    }

    private void deleteKey(Tenant tenant) {
        try {
            HdfsUtils.deleteKey(yarnConfiguration, CustomerSpace.parse(tenant.getId()).getContractId());
        } catch (Exception e) {
            log.error(String.format("Failed to cleanup key for customer %s", tenant.getId()), e);
        }
    }

    private void cleanupTenantInDocumentStores(Tenant tenant) {
        log.info("Clean up tenant in document stores: " + tenant.getId());
        String tenantId = CustomerSpace.parse(tenant.getId()).getTenantId();
        multiTenantDocStores.forEach((tbl, col) -> cleanupTenantInDocumentStore(tenantId, tbl, col));
    }

    private boolean cleanupTenantInDocumentStore(String tenantId, String table, String tenantIdCol) {
        log.info("Cleaning up error tenant " + tenantId + " in document store " + table + ":" + tenantIdCol);
        docJdbcTemplate.execute("DELETE FROM `" + table + "` WHERE `" + tenantIdCol + "` = '" + tenantId + "'");
        return true;
    }

    private void cleanupTenantInGA(Tenant tenant) {
        try {
            log.info("Clean up tenant in GA: " + tenant.getId());
            tenantService.discardTenant(tenant);
        } catch (Exception e) {
            log.error("Failed to clean up GA tenant " + tenant.getId(), e);
        }
    }

    private void cleanupZK() throws Exception {
        try {
            List<AbstractMap.SimpleEntry<Document, Path>> entries = camille
                    .getChildren(PathBuilder.buildContractsPath(podId));
            if (entries != null) {
                for (AbstractMap.SimpleEntry<Document, Path> entry : entries) {
                    Path path = entry.getValue();
                    String contract = path.getSuffix();
                    if (errorTenants.contains(CustomerSpace.parse(contract).getTenantId())) {
                        try {
                            cleanupTenantInZK(contract);
                        } catch (NumberFormatException e) {
                            log.error("Failed to parse timestamp from error tenant id " + contract);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to clean up error tenants in ZK.", e);
        }
    }

    private void cleanupTenantInZK(String contractId) throws Exception {
        log.info("Clean up tenant in ZK: " + contractId);
        Path contractPath = PathBuilder.buildContractPath(podId, contractId);
        if (camille.exists(contractPath)) {
            camille.delete(contractPath);
        }
    }

    private void cleanupTenantsInHdfs() {
        String contractsPath = PathBuilder.buildContractsPath(podId).toString();
        try {
            List<FileStatus> fileStatuses = HdfsUtils.getFileStatusesForDir(yarnConfiguration, contractsPath,
                    FileStatus::isDirectory);
            for (FileStatus fileStatus : fileStatuses) {
                if (errorTenants.contains(CustomerSpace.parse(fileStatus.getPath().getName()).getTenantId())) {
                    String contractId = fileStatus.getPath().getName();
                    log.info("Found an old error contract " + contractId);
                    try {
                        cleanupTenantInHdfs(contractId);
                        cleanupTenantInDL(contractId);
                        cleanupTenantInZK(contractId);
                    } catch (Exception e) {
                        // ignore
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to clean up error tenants in hdfs.", e);
        }
    }

    private void cleanupTenantInHdfs(String contractId) throws Exception {
        if (errorTenants.contains(contractId)) {
            log.info("Clean up contract in HDFS: " + contractId);
            String customerSpace = CustomerSpace.parse(contractId).toString();
            String contractPath = PathBuilder.buildContractPath(podId, contractId).toString();
            if (HdfsUtils.fileExists(yarnConfiguration, contractPath)) {
                HdfsUtils.rmdir(yarnConfiguration, contractPath);
            }
            String customerPath = new Path(customerBase).append(customerSpace).toString();
            if (HdfsUtils.fileExists(yarnConfiguration, customerPath)) {
                HdfsUtils.rmdir(yarnConfiguration, customerPath);
            }
            contractPath = new Path(customerBase).append(contractId).toString();
            if (HdfsUtils.fileExists(yarnConfiguration, contractPath)) {
                HdfsUtils.rmdir(yarnConfiguration, contractPath);
            }
        }
    }

    private void cleanupTenantsInDocumentStores() {
        multiTenantDocStores.forEach(this::cleanupTenantsInDocumentStore);
    }

    private void cleanupTenantsInDocumentStore(String table, String col) {
        log.info("Cleaning up all error tenants in document store " + table + ":" + col);
        List<String> docs = docJdbcTemplate.queryForList(
                "SElECT `" + col + "` FROM  `" + table + "` WHERE `" + col + "` LIKE 'LETest%'", String.class);
        Flux.fromIterable(docs)
                .parallel()
                .runOn(Schedulers.newParallel("ga-cleanup")) //
                .filter(tid -> errorTenants.contains(CustomerSpace.parse(tid).getTenantId()))
                .map(tid -> cleanupTenantInDocumentStore(tid, table, col)) //
                .sequential()
                .retry(2) //
                .onErrorReturn(false) //
                .log(log.getName()) //
                .blockLast();
    }

    private void cleanupTenantInDL(String tenantName) {
        log.info("Clean up error tenant " + tenantName + " from DL.");

        try {
            String permStoreUrl = adminApiHostPort + "/admin/internal/BODCDEVVINT207/BODCDEVVINT187/" + tenantName;
            magicRestTemplate.delete(permStoreUrl);
            log.info("Cleanup VDB permstore for tenant " + tenantName);
        } catch (Exception e) {
            log.error("Failed to clean up permstore for vdb " + tenantName + " : " + errorHandler.getStatusCode() + ", "
                    + errorHandler.getResponseString());
        }

        try {
            TenantDocument tenantDoc = adminTenantProxy.getTenant(tenantName);
            String dlUrl = tenantDoc.getSpaceConfig().getDlAddress();
            DeleteVisiDBDLRequest request = new DeleteVisiDBDLRequest(tenantName, "3");
            InstallResult result = dataLoaderService.deleteDLTenant(request, dlUrl, true);
            log.info("Delete DL tenant " + tenantName + " result=" + JsonUtils.serialize(result));
        } catch (Exception e) {
            log.error("Failed to clean up dl tenant " + tenantName + " : " + errorHandler.getStatusCode() + ", "
                    + errorHandler.getResponseString());
        }
    }

    private void cleanupRedshift() {
        try {
            List<String> tables = redshiftService.getTables(TestFrameworkUtils.TENANTID_PREFIX);
            if (tables != null && !tables.isEmpty()) {
                log.info(String.format("Found %d error tenant tables in redshift.", tables.size()));
                for (String table : tables) {
                    String tenant = RedshiftUtils.extractTenantFromTableName(table);
                    if (errorTenants.contains(CustomerSpace.parse(tenant).getTenantId())) {
                        log.info("Dropping redshift table " + table);
                        try {
                            redshiftService.dropTable(table);
                        } catch (Exception e) {
                            log.error("Failed to drop redshift table " + table, e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to clean up error tenants in redshift.", e);
        }
    }

    private void cleanupS3() {
        try {
            log.info("Start cleaning up S3");
            List<String> folders = s3Service.listSubFolders(customerBucket, "/");
            folders.forEach(folder -> {
                if (errorTenants.contains(CustomerSpace.parse(folder).getTenantId())) {
                    log.info("Removing S3 folder " + folder);
                    try {
                        s3Service.cleanupPrefix(customerBucket, folder);
                    } catch (Exception e) {
                        log.error("Failed to remove S3 folder " + folder, e);
                    }
                }
            });
        } catch (Exception e) {
            log.error("Failed to clean up error tenants in S3.", e);
        }
    }

    @SuppressWarnings("rawtypes")
    private Map<String, String> findAllMultiTenantDocStores() {
        Reflections reflections = new Reflections("com.latticeengines.documentdb.entity");

        List<Class<? extends BaseMultiTenantDocEntity>> allClasses = reflections.getSubTypesOf(BaseMultiTenantDocEntity.class)
                .stream().filter(clz -> !Modifier.toString(clz.getModifiers()).contains("abstract"))
                .collect(Collectors.toList());

        Map<String, String> tables = new HashMap<>();
        allClasses.forEach(clz -> {
            log.info("Found a multi-tenant entity class " + clz);
            Field tenantIdField = FieldUtils.getFieldsListWithAnnotation(clz, TenantIdColumn.class).get(0);
            if (tenantIdField != null) {
                String tenantIdColumn = findTenantIdColumn(tenantIdField);
                log.info("Found tenantId column name for " + clz + " : " + tenantIdColumn);
                String tableName = findTableName(clz);
                log.info("Found table name for " + clz + " : " + tableName);
                if (StringUtils.isNotBlank(tableName) && StringUtils.isNotBlank(tenantIdColumn)) {
                    tables.put(tableName, tenantIdColumn);
                } else {
                    log.warn("Either table name or tenant id column is null: tableName=" + tableName + " tenantIdCol="
                            + tenantIdColumn);
                }
            }
        });
        return tables;
    }

    private String findTenantIdColumn(Field field) {
        Annotation[] annotations = field.getAnnotations();
        for (Annotation annotation : annotations) {
            Class<? extends Annotation> type = annotation.annotationType();
            if (type.getName().equals("javax.persistence.Column")) {
                for (Method method : type.getDeclaredMethods()) {
                    if ("name".equals(method.getName())) {
                        try {
                            return (String) method.invoke(annotation, (Object[]) null);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }
        return null;
    }

    @SuppressWarnings("rawtypes")
    private String findTableName(Class<? extends BaseMultiTenantDocEntity> clz) {
        Annotation[] annotations = clz.getAnnotations();
        for (Annotation annotation : annotations) {
            Class<? extends Annotation> type = annotation.annotationType();
            if (type.getName().equals("javax.persistence.Table")) {
                for (Method method : type.getDeclaredMethods()) {
                    if ("name".equals(method.getName())) {
                        try {
                            Object value = method.invoke(annotation, (Object[]) null);
                            return (value != null) ? (String) value : null;
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }
        return null;
    }
}
