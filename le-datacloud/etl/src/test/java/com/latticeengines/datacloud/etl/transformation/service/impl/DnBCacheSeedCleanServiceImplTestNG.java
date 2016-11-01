package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.DnBCacheSeed;
import com.latticeengines.datacloud.core.source.impl.DnBCacheSeedRaw;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;

public class DnBCacheSeedCleanServiceImplTestNG
        extends TransformationServiceImplTestNGBase<BasicTransformationConfiguration> {
    private static final Log log = LogFactory.getLog(DnBCacheSeedCleanServiceImplTestNG.class);

    private static final String DUNS_NUMBER = "DUNS_NUMBER";
    private static final String LE_DOMAIN = "LE_DOMAIN";
    private static final String LE_NUMBER_OF_LOCATIONS = "LE_NUMBER_OF_LOCATIONS";
    private static final String SALES_VOLUME_LOCAL_CURRENCY = "SALES_VOLUME_LOCAL_CURRENCY";
    private static final String SALES_VOLUME_US_DOLLARS = "SALES_VOLUME_US_DOLLARS";
    private static final String EMPLOYEES_HERE = "EMPLOYEES_HERE";
    private static final String EMPLOYEES_TOTAL = "EMPLOYEES_TOTAL";
    private static final String NUMBER_OF_FAMILY_MEMBERS = "NUMBER_OF_FAMILY_MEMBERS";

    @Autowired
    DnBCacheSeed source;

    @Autowired
    DnBCacheSeedRaw baseSource;

    @Autowired
    private DnBCacheSeedCleanService dnbCacheSeedCleanService;

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
    TransformationService<BasicTransformationConfiguration> getTransformationService() {
        return dnbCacheSeedCleanService;
    }

    @Override
    Source getSource() {
        return source;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructRawDir(source.getBaseSources()[0]).append(baseSourceVersion).toString();
    }

    @Override
    protected void uploadBaseAvro(Source baseSource, String baseSourceVersion) {
        InputStream baseAvroStream = ClassLoader
                .getSystemResourceAsStream("sources/" + baseSource.getSourceName() + ".avro");
        String targetPath = hdfsPathBuilder.constructRawDir(baseSource).append(baseSourceVersion)
                .append("part-0000.avro").toString();
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, targetPath)) {
                HdfsUtils.rmdir(yarnConfiguration, targetPath);
            }
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, baseAvroStream, targetPath);
            InputStream stream = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8));
            String successPath = hdfsPathBuilder.constructRawDir(baseSource).append(baseSourceVersion)
                    .append("_SUCCESS").toString();
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, stream, successPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        hdfsSourceEntityMgr.setCurrentVersion(baseSource, baseSourceVersion);
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
            String duns = String.valueOf(record.get(DUNS_NUMBER));
            String domain = String.valueOf(record.get(LE_DOMAIN));
            Integer numberOfLocation = (Integer) record.get(LE_NUMBER_OF_LOCATIONS);
            Long salesVolumnLocalCurrency = (Long) record.get(SALES_VOLUME_LOCAL_CURRENCY);
            Long salesVolumnUSDollars = (Long) record.get(SALES_VOLUME_US_DOLLARS);
            Integer employeeHere = (Integer) record.get(EMPLOYEES_HERE);
            Integer employeeTotal = (Integer) record.get(EMPLOYEES_TOTAL);
            Integer numberOfFamilyMembers = (Integer) record.get(NUMBER_OF_FAMILY_MEMBERS);
            log.info(DUNS_NUMBER + "=" + duns + " " + LE_DOMAIN + "=" + domain + " " + LE_NUMBER_OF_LOCATIONS + "="
                    + numberOfLocation + " " + SALES_VOLUME_LOCAL_CURRENCY + "=" + salesVolumnLocalCurrency + " "
                    + SALES_VOLUME_US_DOLLARS + "=" + salesVolumnUSDollars + " " + EMPLOYEES_HERE + "=" + employeeHere
                    + " " + EMPLOYEES_TOTAL + "=" + employeeTotal + " " + NUMBER_OF_FAMILY_MEMBERS + "="
                    + numberOfFamilyMembers);
            Assert.assertTrue(duns.equals("01") && domain.equals("google.com") && numberOfLocation.equals(10)
                    && salesVolumnLocalCurrency.equals(100L) && salesVolumnUSDollars.equals(1000L)
                    && employeeHere.equals(10000) && employeeTotal.equals(100000)
                    && numberOfFamilyMembers.equals(1000000));
            rowNum++;
        }
        Assert.assertEquals(rowNum, 1);
    }
}
