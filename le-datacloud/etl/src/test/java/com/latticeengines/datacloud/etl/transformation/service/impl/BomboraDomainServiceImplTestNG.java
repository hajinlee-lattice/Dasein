package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.Iterator;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.BomboraDepivoted;
import com.latticeengines.datacloud.core.source.impl.BomboraDomain;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;

public class BomboraDomainServiceImplTestNG
        extends TransformationServiceImplTestNGBase<BasicTransformationConfiguration> {
    private static final Log log = LogFactory.getLog(BomboraDomainServiceImplTestNG.class);

    private static final String ID = "ID";
    private static final String DOMAIN = "Domain";
    private static final String TIMESTAMP = "LE_Last_Upload_Date";

    @Autowired
    BomboraDomain source;

    @Autowired
    BomboraDepivoted baseSource;

    @Autowired
    private BomboraDomainService bomboraDomainService;

    @Test(groups = "functional", enabled = false)
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
    TransformationService<BasicTransformationConfiguration> getTransformationService() {
        return bomboraDomainService;
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
            String domain = String.valueOf(record.get(DOMAIN));
            Integer id = (Integer) record.get(ID);
            Long timestamp = (Long) record.get(TIMESTAMP);

            Assert.assertTrue((domain.equals("null") && id.equals(2) && timestamp == null)
                    || (domain.equals("google.com") && id.equals(1) && timestamp != null)
                    || (domain.equals("uber.com") && id.equals(3) && timestamp != null));

            rowNum++;
        }
        Assert.assertEquals(rowNum, 3);
    }
}
