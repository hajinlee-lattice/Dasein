package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.AccountMasterSeedMerged;
import com.latticeengines.datacloud.core.source.impl.DnBCacheSeed;
import com.latticeengines.datacloud.core.source.impl.LatticeCacheSeed;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.TransformationStepConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

public class AccountMasterSeedMergeServiceImplTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    private static final Log log = LogFactory.getLog(AccountMasterSeedMergeServiceImplTestNG.class);

    @Autowired
    AccountMasterSeedMerged source;

    @Autowired
    DnBCacheSeed dnBCacheSeed;

    @Autowired
    LatticeCacheSeed latticeCacheSeed;

    @Autowired
    SourceService sourceService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private PipelineTransformationService pipelineTransformationService;

    String targetSourceName = "AccountMasterSeedMerged";

    ObjectMapper om = new ObjectMapper();

    private static final String DUNS = "DUNS";
    private static final String DOMAIN = "Domain";
    private static final String NAME = "Name";
    private static final String LE_IS_PRIMARY_DOMAIN = "LE_IS_PRIMARY_DOMAIN";
    private static final String LE_IS_PRIMARY_LOCATION = "LE_IS_PRIMARY_LOCATION";
    private static final String LE_NUMBER_OF_LOCATIONS = "LE_NUMBER_OF_LOCATIONS";
    private static final String LE_PRIMARY_DUNS = "LE_PRIMARY_DUNS";
    private static final String PRIMARY_INDUSTRY = "PrimaryIndustry";
    private static final String DOMAIN_SOURCE = "DomainSource";

    @Test(groups = "functional", enabled = true)
    public void testTransformation() {
        uploadBaseSourceFile(dnBCacheSeed, "DnBCacheSeed_TestAccountMasterSeedMerged", baseSourceVersion);
        uploadBaseSourceFile(latticeCacheSeed, "LatticeCacheSeed", baseSourceVersion);
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    TransformationService<PipelineTransformationConfiguration> getTransformationService() {
        return pipelineTransformationService;
    }

    @Override
    Source getSource() {
        return source;
    }

    @Override
    String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    @Override
    PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();

            configuration.setName("AccountMasterSeedMerge");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add("DnBCacheSeed");
            baseSources.add("LatticeCacheSeed");
            step1.setBaseSources(baseSources);
            step1.setTransformer("accountMasterSeedMergeTransformer");
            step1.setTargetSource(targetSourceName);
            String confParamStr1 = getTransformerConfig();
            step1.setConfiguration(confParamStr1);

            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);

            // -----------
            configuration.setSteps(steps);

            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String getTransformerConfig() throws JsonProcessingException {
        TransformerConfig conf = new TransformerConfig();
        conf.setTransformer("accountMasterSeedMergeTransformer");
        return om.writeValueAsString(conf);
    }

    @Override
    String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    @Override
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        Object[][] expectedResults = new Object[][] {
                { "a.com", "111", "111", "Y", "Y", 2, "DnBName111", "DnBPI111", "LE" },
                { "b.com", "222", "222", "Y", "Y", 3, "DnBName222", "DnBPI222", "LE" },
                { "d.com", "333", "333", "Y", "Y", 4, "DnBName333", "DnBPI333", "LE" },
                { "c.com", "333", "333", "N", "Y", 4, "DnBName333", "DnBPI333", "DnB" },
                { "e.com", "444", "444", "Y", "Y", 5, "DnBName444", "DnBPI444", "LE" },
                { "e.com", "555", "444", "Y", "N", 6, "DnBName555", "DnBPI555", "LE" },
                { "f.com", "666", "666", "Y", "Y", 7, "DnBName666", "DnBPI666", "LE" },
                { "f.com", "777", "666", "Y", "N", 8, "DnBName777", "DnBPI777", "LE" },
                { "g.com", "888", "999", "Y", "N", 9, "DnBName888", "DnBPI888", "LE" },
                { "g.com", "999", "999", "Y", "Y", 10, "DnBName999", "DnBPI999", "LE" },
                { "g.com", "101010", "999", "Y", "N", 11, "DnBName101010", "DnBPI101010", "LE" },
                { "i.com", "111111", "121212", "Y", "N", 12, "DnBName111111", "DnBPI111111", "LE" },
                { "ii.com", "111111", "121212", "Y", "N", 12, "DnBName111111", "DnBPI111111", "LE" },
                { "h.com", "121212", "121212", "N", "Y", 13, "DnBName121212", "DnBPI121212", "DnB" },
                { "i.com", "121212", "121212", "Y", "Y", 13, "DnBName121212", "DnBPI121212", "LE" },
                { "ii.com", "121212", "121212", "Y", "Y", 13, "DnBName121212", "DnBPI121212", "LE" },
                { "h.com", "131313", "121212", "N", "N", 14, "DnBName131313", "DnBPI131313", "DnB" },
                { "i.com", "131313", "121212", "Y", "N", 14, "DnBName131313", "DnBPI131313", "LE" },
                { "ii.com", "131313", "121212", "Y", "N", 14, "DnBName131313", "DnBPI131313", "LE" },
                { "a.com", "NoDu111", "null", "Y", "Y", 15, "DnBNameNoDu111", "DnBPINoDu111", "LE" },
                { "b.com", "NoDu222", "null", "N", "Y", 16, "DnBNameNoDu222", "DnBPINoDu222", "DnB" },
                { "c.com", "NoDu222", "null", "Y", "Y", 16, "DnBNameNoDu222", "DnBPINoDu222", "LE" },
                { "d.com", "NoDu222", "null", "Y", "Y", 16, "DnBNameNoDu222", "DnBPINoDu222", "LE" },
                { "b.com", "NoDu333", "null", "Y", "Y", 17, "DnBNameNoDu333", "DnBPINoDu333", "DnB" },
                { "c.com", "NoDu444", "null", "N", "Y", 17, "DnBNameNoDu444", "DnBPINoDu444", "DnB" },
                { "null", "NoDu555", "null", "N", "Y", 17, "DnBNameNoDu555", "DnBPINoDu555", "DnB" },
                { "null", "NoDu666", "null", "Y", "Y", 17, "DnBNameNoDu666", "DnBPINoDu666", "DnB" },
                { "j.com", "null", "null", "Y", "Y", 1, "LeNamej.com", "LePIj.com", "LE" },
                { "k.com", "null", "null", "Y", "Y", 1, "LeNamek.com", "LePIk.com", "LE" },
                { "l.com", "null", "null", "Y", "Y", 1, "LeNamel.com", "LePIl.com", "LE" } };
        int rowNum = 0;
        Set<String> set = new HashSet<>();
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String domain = String.valueOf(record.get(DOMAIN));
            String duns = String.valueOf(record.get(DUNS));
            String duDuns = String.valueOf(record.get(LE_PRIMARY_DUNS));
            String isPrimaryDomain = String.valueOf(record.get(LE_IS_PRIMARY_DOMAIN));
            String isPrimaryLocation = String.valueOf(record.get(LE_IS_PRIMARY_LOCATION));
            Integer numberOfLocation = (Integer) record.get(LE_NUMBER_OF_LOCATIONS);
            String name = String.valueOf(record.get(NAME));
            String primaryIndustry = String.valueOf(record.get(PRIMARY_INDUSTRY));
            String domainSource = String.valueOf(record.get(DOMAIN_SOURCE));
            Assert.assertFalse(set.contains(domain + duns));    // To verify domain + duns is unique in AccountMasterSeed
            set.add(domain + duns);
            log.info(String.format(
                    "Domain = %s, Duns = %s, DuDuns = %s, IsPrimaryDomain = %s, IsPrimaryLocation = %s, NumberOfLocation = %d, Name = %s, PrimaryIndustry = %s, DomainSource = %s",
                    domain, duns, duDuns, isPrimaryDomain, isPrimaryLocation, numberOfLocation, name, primaryIndustry,
                    domainSource));
            boolean flag = false;
            for (Object[] expectedResult : expectedResults) {
                if (domain.equals(expectedResult[0]) && duns.equals(expectedResult[1])
                        && duDuns.equals(expectedResult[2]) && isPrimaryDomain.equals(expectedResult[3])
                        && isPrimaryLocation.equals(expectedResult[4]) && numberOfLocation.equals(expectedResult[5])
                        && name.equals(expectedResult[6]) && primaryIndustry.equals(expectedResult[7])
                        && domainSource.equals(expectedResult[8])) {
                    flag = true;
                    break;
                }
            }
            Assert.assertTrue(flag);
            rowNum++;
        }
        Assert.assertEquals(rowNum, 30);
    }

}
