package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.AccountMasterSeed;
import com.latticeengines.datacloud.core.source.impl.DnBCacheSeed;
import com.latticeengines.datacloud.core.source.impl.LatticeCacheSeed;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;

public class AccountMasterSeedRebuildServiceImplTestNG
        extends TransformationServiceImplTestNGBase<BasicTransformationConfiguration> {
    private static final Log log = LogFactory.getLog(AccountMasterSeedRebuildServiceImplTestNG.class);

    private String baseSourceVersionDnB = "2016-10-01_00-00-00_UTC";
    private String baseSourceVersionLattice = "2016-09-01_00-00-00_UTC";
    private String targetVersion = "2016-10-01_00-00-00_UTC";

    @Autowired
    AccountMasterSeed source;

    @Autowired
    DnBCacheSeed dnBCacheSeed;

    @Autowired
    LatticeCacheSeed latticeCacheSeed;

    @Autowired
    private AccountMasterSeedRebuildService accountMasterSeedRebuildService;

    @Test(groups = "functional")
    public void testTransformation() {
        uploadBaseAvro(dnBCacheSeed, baseSourceVersionDnB);
        uploadBaseAvro(latticeCacheSeed, baseSourceVersionLattice);
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    TransformationService<BasicTransformationConfiguration> getTransformationService() {
        return accountMasterSeedRebuildService;
    }

    @Override
    Source getSource() {
        return source;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return null;
    }

    @Override
    BasicTransformationConfiguration createTransformationConfiguration() {
        BasicTransformationConfiguration configuration = new BasicTransformationConfiguration();
        configuration.setVersion(targetVersion);
        List<String> baseVersions = new ArrayList<String>();
        baseVersions.add(baseSourceVersionDnB);
        baseVersions.add(baseSourceVersionLattice);
        configuration.setBaseVersions(baseVersions);
        return configuration;
    }

    @Override
    protected String getPathForResult() {
        return hdfsPathBuilder.constructSnapshotDir(source, targetVersion).toString();
    }

    @Override
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {
    }
}
