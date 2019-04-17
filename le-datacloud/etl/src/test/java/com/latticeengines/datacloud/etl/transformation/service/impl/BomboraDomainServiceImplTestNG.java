package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.BomboraDepivoted;
import com.latticeengines.datacloud.core.source.impl.BomboraDomain;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.BasicTransformationConfiguration;

public class BomboraDomainServiceImplTestNG
        extends TransformationServiceImplTestNGBase<BasicTransformationConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(BomboraDomainServiceImplTestNG.class);

    private static final String ID = "ID";
    private static final String DOMAIN = "Domain";
    private static final String TIMESTAMP = "LE_Last_Upload_Date";

    @Autowired
    BomboraDomain source;

    @Autowired
    BomboraDepivoted baseSource;

    @Autowired
    private BomboraDomainService bomboraDomainService;

    @Test(groups = "pipeline2", enabled = true)
    public void testTransformation() {
        uploadBaseAvro(baseSource, baseSourceVersion);
        uploadBaseAvro(source, baseSourceVersion);
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected TransformationService<BasicTransformationConfiguration> getTransformationService() {
        return bomboraDomainService;
    }

    @Override
    protected Source getSource() {
        return source;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(source.getBaseSources()[0].getSourceName(), baseSourceVersion)
                .toString();
    }

    @Override
    protected BasicTransformationConfiguration createTransformationConfiguration() {
        BasicTransformationConfiguration configuration = new BasicTransformationConfiguration();
        configuration.setVersion(targetVersion);
        return configuration;
    }

    @Override
    protected String getPathForResult() {
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        int rowNum = 0;
        Set<Long> ids = new HashSet<Long>();
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String domain = String.valueOf(record.get(DOMAIN));
            Long id = (Long) record.get(ID);
            Long timestamp = (Long) record.get(TIMESTAMP);
            if (id != null) {
                ids.add(id);
                log.info(String.valueOf(id));
            }
            Assert.assertTrue(
                    (domain.equals("null") && timestamp == null) || (domain.equals("google.com") && timestamp != null)
                            || (domain.equals("uber.com") && timestamp != null));

            rowNum++;
        }
        Assert.assertEquals(rowNum, 3);
        Assert.assertEquals(ids.size(), rowNum);
    }
}
