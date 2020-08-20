package com.latticeengines.datacloud.yarn.service.impl;

import static com.latticeengines.domain.exposed.datacloud.match.config.ExclusionCriterion.OutOfBusiness;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.yarn.exposed.service.DataCloudYarnService;
import com.latticeengines.datacloud.yarn.testframework.DataCloudYarnFunctionalTestNGBase;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.DataCloudJobConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusMatchConfig;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusMatchRule;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

public class PrimeMatchYarnTestNG extends DataCloudYarnFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PrimeMatchYarnTestNG.class);

    private static final String avroDir = "/tmp/PrimeMatchYarnTestNG";
    private static final String podId = "PrimeMatchYarnTestNG";

    @Inject
    private DataCloudYarnService dataCloudYarnService;

    @Inject
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Value("${datacloud.match.default.decision.graph.prime}")
    private String primeMatchDG;

    @BeforeClass(groups = {"functional", "manual"})
    public void setup() throws Exception {
        switchHdfsPod(podId);
        if (HdfsUtils.fileExists(yarnConfiguration, hdfsPathBuilder.podDir().toString())) {
            HdfsUtils.rmdir(yarnConfiguration, hdfsPathBuilder.podDir().toString());
        }
    }

    @Test(groups = "functional")
    public void testPrimeMatch() {
        String fileName = "BulkMatchInput.avro";
        cleanupAvroDir(avroDir);
        uploadDataCsv(avroDir, fileName);
        String avroPath = avroDir + "/" + fileName;

        DataCloudJobConfiguration jobConfiguration = jobConfiguration(avroPath);

        ApplicationId applicationId = dataCloudYarnService.submitPropDataJob(jobConfiguration);
        FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnClient, applicationId);
        Assert.assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        String avroGlob = getBlockOutputDir(jobConfiguration) + "/*.avro";
        Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroGlob);
        log.info("Fields: {}", StringUtils.join(schema.getFields().stream() //
                .map(Schema.Field::name).collect(Collectors.toList()), ","));
        Iterator<GenericRecord> records = AvroUtils.iterateAvroFiles(yarnConfiguration, avroGlob);
        long count = 0L;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            Assert.assertNotNull(record.get(InterfaceName.InternalId.name()));
            count++;
        }
        Assert.assertTrue(count > 0);
    }

    private String getBlockOutputDir(DataCloudJobConfiguration jobConfiguration) {
        String rootUid = jobConfiguration.getRootOperationUid();
        String blockUid = jobConfiguration.getBlockOperationUid();
        return hdfsPathBuilder.constructMatchBlockDir(rootUid, blockUid).toString();
    }

    private String getBlockCandidateOutputDir(DataCloudJobConfiguration jobConfiguration) {
        String rootUid = jobConfiguration.getRootOperationUid();
        String blockUid = jobConfiguration.getBlockOperationUid();
        return hdfsPathBuilder.constructMatchBlockCandidateDir(rootUid, blockUid).toString();
    }

    private DataCloudJobConfiguration jobConfiguration(String avroPath) {
        Schema schema = AvroUtils.getSchema(yarnConfiguration, new Path(avroPath));
        Map<MatchKey, List<String>> keyMap = MatchKeyUtils.resolveKeyMap(schema);

        MatchInput matchInput = new MatchInput();

        matchInput.setTenant(new Tenant(DataCloudConstants.SERVICE_TENANT));
        matchInput.setCustomSelection(getColumnSelection());
        matchInput.setDataCloudVersion(versionEntityMgr.currentApprovedVersionAsString());
        matchInput.setKeyMap(keyMap);
        matchInput.setSplitsPerBlock(8);
        matchInput.setDecisionGraph(primeMatchDG);
        matchInput.setUseDnBCache(false);
        matchInput.setUseRemoteDnB(true);
        matchInput.setAllocateId(false);
        matchInput.setEntityKeyMaps(prepareEntityKeyMap());
        matchInput.setTargetEntity(BusinessEntity.PrimeAccount.name());
        matchInput.setOperationalMode(OperationalMode.LDC_MATCH);
        DplusMatchRule baseRule = new DplusMatchRule(7, Collections.singleton("A.{3}A.{3}.*"))
                .exclude(OutOfBusiness) //
                .review(4, 6, Collections.singleton("A.*"));
        DplusMatchConfig dplusMatchConfig = new DplusMatchConfig(baseRule);
        matchInput.setDplusMatchConfig(dplusMatchConfig);
        matchInput.setUseDirectPlus(true);

        DataCloudJobConfiguration jobConfiguration = new DataCloudJobConfiguration();
        jobConfiguration.setHdfsPodId(podId);
        jobConfiguration.setName("DataCloudMatchBlock");
        jobConfiguration.setCustomerSpace(CustomerSpace.parse("LDCTest"));
        jobConfiguration.setAvroPath(avroPath);
        jobConfiguration.setBlockSize(AvroUtils.count(yarnConfiguration, avroPath).intValue());
        jobConfiguration.setRootOperationUid(UUID.randomUUID().toString().toUpperCase());
        jobConfiguration.setBlockOperationUid(UUID.randomUUID().toString().toUpperCase());
        jobConfiguration.setThreadPoolSize(4);
        jobConfiguration.setGroupSize(10);
        jobConfiguration.setMatchInput(matchInput);

        return jobConfiguration;
    }

    private Map<String, MatchInput.EntityKeyMap> prepareEntityKeyMap() {
        Map<String, MatchInput.EntityKeyMap> entityKeyMaps = new HashMap<>();
        // Both Account & Contact match needs Account key map
        MatchInput.EntityKeyMap entityKeyMap = new MatchInput.EntityKeyMap();
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.DUNS, Collections.singletonList("DUNS"));
        keyMap.put(MatchKey.Name, Collections.singletonList("CompanyName"));
        keyMap.put(MatchKey.State, Collections.singletonList("State"));
        keyMap.put(MatchKey.Country, Collections.singletonList("Country"));
        entityKeyMap.setKeyMap(keyMap);
        entityKeyMaps.put(BusinessEntity.Account.name(), entityKeyMap);
        return entityKeyMaps;
    }

    private ColumnSelection getColumnSelection() {
        List<Column> columns = Stream.of( //
                "duns_number", //
                "primaryname", //
                "tradestylenames_name", //
                "primaryaddr_street_line1", //
                "primaryaddr_street_line2", //
                "primaryaddr_addrlocality_name", //
                "primaryaddr_addrregion_name", //
                "primaryaddr_postalcode", //
                "primaryaddr_country_name", //
                "telephone_telephonenumber", //
                "primaryindcode_ussicv4" //
        ).map(Column::new).collect(Collectors.toList());
        ColumnSelection cs = new ColumnSelection();
        cs.setColumns(columns);
        return cs;
    }

}
