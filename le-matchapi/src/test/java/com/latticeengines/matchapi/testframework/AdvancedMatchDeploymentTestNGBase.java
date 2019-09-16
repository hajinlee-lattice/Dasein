package com.latticeengines.matchapi.testframework;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.InputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.entity.BumpVersionRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.BumpVersionResponse;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishStatistics;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.security.exposed.service.TenantService;

public class AdvancedMatchDeploymentTestNGBase extends MatchapiDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(AdvancedMatchDeploymentTestNGBase.class);

    @Inject
    protected MatchProxy matchProxy;

    @Inject
    protected TenantService tenantService;

    @Inject
    private DataCloudVersionEntityMgr versionEntityMgr;

    protected String testTenantId;
    protected Tenant testTenant;
    protected String currentDataCloudVersion;

    @BeforeClass(groups = "deployment")
    protected void init() {
        HdfsPodContext.changeHdfsPodId(getPodId());
        cleanupAvroDir(hdfsPathBuilder.podDir().toString());

        currentDataCloudVersion = versionEntityMgr.currentApprovedVersionAsString();

        testTenant = getTenant();
        testTenant.setName(testTenant.getId());
        testTenantId = testTenant.getId();
        tenantService.registerTenant(testTenant);
        // populate pid so that the tenant could be deleted in destroy()
        testTenant = tenantService.findByTenantId(testTenantId);

        log.info("Instantiated test tenant (ID={}). AvroDir={}, DataCloudVersion={}", testTenantId, getAvroDir(),
                currentDataCloudVersion);
    }

    @AfterClass(groups = "deployment")
    protected void destroy() {
        tenantService.discardTenant(testTenant);
    }

    /*-
     * retrieve a map of keyCol -> EntityId
     */
    protected Map<String, String> getEntityIdMap(@NotNull String keyColName, @NotNull List<GenericRecord> records) {
        return records.stream() //
                .filter(Objects::nonNull) //
                .filter(record -> getStrValue(record, keyColName) != null) //
                .filter(record -> getStrValue(record, InterfaceName.EntityId.name()) != null) //
                .map(record -> Pair.of(getStrValue(record, keyColName),
                        getStrValue(record, InterfaceName.EntityId.name()))) //
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue, (v1, v2) -> v1));
    }

    protected List<GenericRecord> getRecords(String outputPath, boolean dirExists, boolean checkEntityId)
            throws Exception {
        if (dirExists) {
            Assert.assertTrue(HdfsUtils.isDirectory(yarnConfiguration, outputPath),
                    String.format("%s should be an existing directory", outputPath));
        }

        if (!HdfsUtils.isDirectory(yarnConfiguration, outputPath)) {
            return Collections.emptyList();
        }

        List<GenericRecord> records = new ArrayList<>();
        Iterator<GenericRecord> it = AvroUtils.iterator(yarnConfiguration, outputPath + "/*.avro");
        for (int i = 0; it.hasNext(); i++) {
            GenericRecord record = it.next();
            Assert.assertNotNull(record, String.format("Record at index %d is null", i));

            String entityId = getStrValue(record, InterfaceName.EntityId.name());
            if (checkEntityId) {
                Assert.assertNotNull(entityId, "EntityId should not be null");
            }
            records.add(record);
        }
        return records;
    }

    protected int publishEntity(String entity) {
        return publishEntity(entity, null);
    }

    /*
     * publish specific entity from staging to serving (with specified version),
     * return number of seeds published
     */
    protected int publishEntity(String entity, Integer servingVersion) {
        log.info("Publish entity {} for tenant {} from staging to serving (version={})", entity, testTenantId,
                servingVersion);
        EntityPublishRequest request = new EntityPublishRequest();
        request.setEntity(entity);
        request.setSrcTenant(testTenant);
        request.setDestTenant(testTenant);
        request.setDestVersion(servingVersion);
        request.setDestEnv(EntityMatchEnvironment.SERVING);
        request.setDestTTLEnabled(true);
        request.setBumpupVersion(false);
        List<EntityPublishStatistics> result = matchProxy.publishEntity(Collections.singletonList(request));
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
        Assert.assertNotNull(result.get(0));
        return result.get(0).getSeedCount();
    }

    protected void bumpStagingVersion() {
        BumpVersionRequest request = new BumpVersionRequest();
        request.setTenant(testTenant);
        request.setEnvironments(Collections.singletonList(EntityMatchEnvironment.STAGING));
        BumpVersionResponse response = matchProxy.bumpVersion(request);
        Assert.assertNotNull(response);
        log.info("Staging version for tenant(ID={}) is {}", testTenantId, response.getVersions());
    }

    protected InputBuffer prepareStringData(String scenario, String[] fields, Object[][] data) {
        String avroDir = getAvroDir();
        cleanupAvroDir(avroDir);
        AvroInputBuffer inputBuffer = new AvroInputBuffer();
        inputBuffer.setAvroDir(avroDir);
        uploadAvroData(data, Arrays.asList(fields),
                Arrays.stream(fields).map(field -> String.class).collect(Collectors.toList()), avroDir,
                String.format("%s.avro", scenario));
        return inputBuffer;
    }

    protected String getStrValue(GenericRecord record, String col) {
        if (record == null || col == null) {
            return null;
        }

        return record.get(col) == null ? null : record.get(col).toString();
    }

    /*
     * helper for test namespaces
     */

    protected String getTenantId() {
        return getClass().getSimpleName() + UUID.randomUUID().toString();
    }

    protected Tenant getTenant() {
        return new Tenant(CustomerSpace.parse(getTenantId()).toString());
    }

    protected String getAvroDir() {
        return String.format("/tmp/%s", getPodId());
    }

    protected String getPodId() {
        return getClass().getSimpleName();
    }
}
