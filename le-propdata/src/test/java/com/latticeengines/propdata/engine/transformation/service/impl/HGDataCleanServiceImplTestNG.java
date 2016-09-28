package com.latticeengines.propdata.engine.transformation.service.impl;

import java.util.Calendar;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.source.impl.HGDataClean;
import com.latticeengines.propdata.core.source.impl.HGDataRaw;
import com.latticeengines.propdata.engine.transformation.configuration.impl.HGDataCleanConfiguration;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;

public class HGDataCleanServiceImplTestNG extends TransformationServiceImplTestNGBase<HGDataCleanConfiguration> {

    private static final Log log = LogFactory.getLog(HGDataCleanServiceImplTestNG.class);

    @Autowired
    HGDataClean source;

    @Autowired
    HGDataRaw baseSource;

    @Autowired
    private HGDataCleanService hgDataCleanService;

    @Test(groups = "functional")
    public void testTransformation() {
        uploadBaseAvro(baseSource, baseSourceVersion);
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    TransformationService<HGDataCleanConfiguration> getTransformationService() {
        return hgDataCleanService;
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
    HGDataCleanConfiguration createTransformationConfiguration() {
        HGDataCleanConfiguration configuration = new HGDataCleanConfiguration();
        configuration.setVersion(targetVersion);
        calendar.set(2016, Calendar.AUGUST, 1);
        configuration.setFakedCurrentDate(calendar.getTime());
        return configuration;
    }

    @Override
    protected String getPathForResult() {
        return hdfsPathBuilder.constructSnapshotDir(source, targetVersion).toString();
    }

    @Override
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        int recordsToCheck = 100;
        int pos = 0;
        Long sixMonths = 6 * TimeUnit.DAYS.toMillis(30);
        while (pos < recordsToCheck && records.hasNext()) {
            GenericRecord record = records.next();
            Long lastVerified = (Long) record.get("Last_Verified_Date");
            Long timeStamp = (Long) record.get("LE_Last_Upload_Date");
            try {
                Assert.assertTrue(timeStamp < lastVerified + sixMonths);
            } catch (Exception e) {
                System.out.println(record);
                throw e;
            }
        }
    }

}
