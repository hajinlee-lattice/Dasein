package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.Iterator;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.AccountMasterLookup;
import com.latticeengines.datacloud.core.source.impl.AccountMasterSeed;
import com.latticeengines.datacloud.core.source.impl.OrbCacheSeedSecondaryDomain;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;

public class AccountMasterLookupRebuildServiceImplTestNG
        extends TransformationServiceImplTestNGBase<BasicTransformationConfiguration> {
    private static final Log log = LogFactory.getLog(AccountMasterLookupRebuildServiceImplTestNG.class);

    private static final String LATTICEID = "LatticeID";
    private static final String KEY = "Key";

    @Autowired
    AccountMasterLookup source;

    @Autowired
    AccountMasterSeed baseSource;

    @Autowired
    OrbCacheSeedSecondaryDomain baseSourceOrbCacheSeedSecondaryDomain;

    @Autowired
    private AccountMasterLookupRebuildService accountMasterLookupRebuildService;

    @Test(groups = "functional")
    public void testTransformation() {
        uploadBaseSourceFile(baseSource, baseSource.getSourceName() + "_Test" + source.getSourceName(),
                baseSourceVersion);
        uploadBaseSourceFile(baseSourceOrbCacheSeedSecondaryDomain,
                baseSourceOrbCacheSeedSecondaryDomain.getSourceName() + "_Test", baseSourceVersion);
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    TransformationService<BasicTransformationConfiguration> getTransformationService() {
        return accountMasterLookupRebuildService;
    }

    @Override
    Source getSource() {
        return source;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(source.getBaseSources()[0], baseSourceVersion).toString();
    }

    @Override
    BasicTransformationConfiguration createTransformationConfiguration() {
        BasicTransformationConfiguration configuration = new BasicTransformationConfiguration();
        configuration.setVersion(targetVersion);
        return configuration;
    }

    @Override
    protected String getPathForResult() {
        return hdfsPathBuilder.constructSnapshotDir(source, targetVersion).toString();
    }

    @Override
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            Long latticeId = (Long) record.get(LATTICEID);
            String key = String.valueOf(record.get(KEY));
            log.info(latticeId + " " + key);

            Assert.assertTrue((latticeId.equals(1L) && key.equals("_DOMAIN_a.com_DUNS_NULL"))
                    || (latticeId.equals(1L) && key.equals("_DOMAIN_secondary.com_DUNS_NULL"))
                    || (latticeId.equals(1L) && key.equals("_DOMAIN_mailserver.com_DUNS_NULL"))
                    || (latticeId.equals(1L) && key.equals("_DOMAIN_a.com_DUNS_01"))
                    || (latticeId.equals(1L) && key.equals("_DOMAIN_mailserver.com_DUNS_01"))
                    || (latticeId.equals(1L) && key.equals("_DOMAIN_secondary.com_DUNS_01"))
                    || (latticeId.equals(1L) && key.equals("_DOMAIN_NULL_DUNS_01"))
                    || (latticeId.equals(2L) && key.equals("_DOMAIN_secondary.com_DUNS_02"))
                    || (latticeId.equals(2L) && key.equals("_DOMAIN_NULL_DUNS_02"))
                    || (latticeId.equals(2L) && key.equals("_DOMAIN_a.com_DUNS_02"))
                    || (latticeId.equals(2L) && key.equals("_DOMAIN_mailserver.com_DUNS_02"))
                    || (latticeId.equals(3L) && key.equals("_DOMAIN_secondary.com_DUNS_03"))
                    || (latticeId.equals(3L) && key.equals("_DOMAIN_mailserver.com_DUNS_03"))
                    || (latticeId.equals(3L) && key.equals("_DOMAIN_NULL_DUNS_03"))
                    || (latticeId.equals(3L) && key.equals("_DOMAIN_a.com_DUNS_03"))
                    || (latticeId.equals(4L) && key.equals("_DOMAIN_b.com_DUNS_01"))
                    || (latticeId.equals(5L) && key.equals("_DOMAIN_b.com_DUNS_02"))
                    || (latticeId.equals(5L) && key.equals("_DOMAIN_b.com_DUNS_NULL"))
                    || (latticeId.equals(6L) && key.equals("_DOMAIN_b.com_DUNS_03"))
                    || (latticeId.equals(7L) && key.equals("_DOMAIN_NULL_DUNS_04"))
                    || (latticeId.equals(7L) && key.equals("_DOMAIN_b.com_DUNS_04"))
                    || (latticeId.equals(8L) && key.equals("_DOMAIN_c.com_DUNS_NULL")));
            rowNum++;
        }
        Assert.assertEquals(rowNum, 22);
    }
}
