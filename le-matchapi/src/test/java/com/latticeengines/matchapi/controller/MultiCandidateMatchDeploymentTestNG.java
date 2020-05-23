package com.latticeengines.matchapi.controller;

import java.util.ArrayList;
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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.log4j.Level;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.match.exposed.service.MatchCommandService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatus;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;
import com.latticeengines.matchapi.testframework.MatchapiDeploymentTestNGBase;

// dpltc deploy -a matchapi,workflowapi
public class MultiCandidateMatchDeploymentTestNG extends MatchapiDeploymentTestNGBase {

    private static final String podId = "PrimeMatchDeploymentTestNG";
    private static final String avroDir = "/tmp/PrimeMatchDeploymentTestNG";
    private static final String fileName = "SourceFile_csv.avro";

    @Inject
    private DataCloudVersionEntityMgr dataCloudVersionEntityMgr;

    @Inject
    private MatchCommandService matchCommandService;

    @Value("${datacloud.match.latest.data.cloud.major.version}")
    private String latestMajorVersion;

    @Value("${datacloud.match.default.decision.graph.prime}")
    private String primeMatchDG;

    @Test(groups = "deployment")
    public void testBulkMatch() {
        String version = dataCloudVersionEntityMgr.latestApprovedForMajorVersion(latestMajorVersion).getVersion();

        HdfsPodContext.changeHdfsPodId(podId);
        cleanupAvroDir(avroDir);
        List<Class<?>> fieldTypes = new ArrayList<>();
        fieldTypes.add(Integer.class);
        fieldTypes.add(String.class);
        fieldTypes.add(String.class);
        fieldTypes.add(String.class);
        fieldTypes.add(String.class);
        fieldTypes.add(String.class);
        uploadDataCsv(avroDir, fileName, "matchinput/BulkMatchInput.csv", fieldTypes, InterfaceName.InternalId.name());

        // use avro file and without schema
        MatchInput input = createAvroBulkMatchInput(false, null, version, avroDir, fileName);
        input.setExcludePublicDomain(true);
        setMatchInput(input, version);

        MatchCommand command = matchProxy.matchBulk(input, podId);
        ApplicationId appId = ApplicationIdUtils.toApplicationIdObj(command.getApplicationId());
        FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnClient, appId);
        Assert.assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        MatchCommand matchCommand = matchCommandService.getByRootOperationUid(command.getRootOperationUid());
        Assert.assertEquals(matchCommand.getMatchStatus(), MatchStatus.FINISHED);
        Assert.assertEquals(matchCommand.getRowsMatched(), Integer.valueOf(100));

        String candidateLocation = matchCommand.getCandidateLocation();
        Assert.assertNotNull(candidateLocation);
        String avroGlob = PathUtils.toAvroGlob(candidateLocation);
        Iterator<GenericRecord> records = AvroUtils.iterateAvroFiles(yarnConfiguration, avroGlob);
        long count = 0L;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            Assert.assertNotNull(record.get(InterfaceName.InternalId.name()));
            count++;
        }
        Assert.assertTrue(count > 0);
        // Assert.assertTrue(count >= matchCommand.getRowsMatched());
    }

    private void setMatchInput(MatchInput matchInput, String version) {
        matchInput.setLogLevelEnum(Level.DEBUG);
        matchInput.setDecisionGraph(primeMatchDG);
        matchInput.setDataCloudVersion(version);
        matchInput.setRootOperationUid(UUID.randomUUID().toString());
        matchInput.setUseDnBCache(false);
        matchInput.setUseRemoteDnB(true);
        matchInput.setAllocateId(false);
        matchInput.setUseDirectPlus(true);
        matchInput.setEntityKeyMaps(prepareEntityKeyMap());
        matchInput.setTargetEntity(BusinessEntity.PrimeAccount.name());
        matchInput.setOperationalMode(OperationalMode.MULTI_CANDIDATES);
        matchInput.setPredefinedSelection(null);
        matchInput.setCustomSelection(getColumnSelection());
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

    private MatchInput createAvroBulkMatchInput(boolean useDir, Schema inputSchema, String dataCloudVersion,
                                                String avroDir, String fileName) {
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant(DataCloudConstants.SERVICE_CUSTOMERSPACE));
        matchInput.setPredefinedSelection(ColumnSelection.Predefined.RTS);
        AvroInputBuffer inputBuffer = new AvroInputBuffer();
        if (useDir) {
            inputBuffer.setAvroDir(avroDir);
        } else {
            inputBuffer.setAvroDir(avroDir + "/" + fileName);
        }
        if (inputSchema != null) {
            inputBuffer.setSchema(inputSchema);
        }
        matchInput.setInputBuffer(inputBuffer);
        matchInput.setDataCloudVersion(dataCloudVersion);
        return matchInput;
    }

    private ColumnSelection getColumnSelection() {
        List<Column> columns = Stream.of(
                DataCloudConstants.ATTR_LDC_DUNS,
                DataCloudConstants.ATTR_LDC_NAME,
                "TRADESTYLE_NAME",
                "LDC_Street",
                "STREET_ADDRESS_2",
                DataCloudConstants.ATTR_CITY,
                DataCloudConstants.ATTR_STATE,
                DataCloudConstants.ATTR_ZIPCODE,
                DataCloudConstants.ATTR_COUNTRY,
                "TELEPHONE_NUMBER",
                "LE_SIC_CODE"
        ).map(Column::new).collect(Collectors.toList());
        ColumnSelection cs = new ColumnSelection();
        cs.setColumns(columns);
        return cs;
    }

}
