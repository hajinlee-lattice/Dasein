package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.BitCodecUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.BuiltWithMostRecent;
import com.latticeengines.datacloud.core.source.impl.BuiltWithTechIndicators;
import com.latticeengines.datacloud.etl.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;

public class BuiltWithTechIndicatorsServiceImplTestNG
        extends TransformationServiceImplTestNGBase<BasicTransformationConfiguration> {

    private static final Log log = LogFactory.getLog(BuiltWithTechIndicatorsServiceImplTestNG.class);

    private final String TECH_INDICATORS = "TechIndicators";

    private final String TECH_UNIX = "TechIndicator_Unix";
    private final String TECH_MOD_SSL = "TechIndicator_mod_ssl";

    private int HAS_UNIX_POS = -1;
    private int HAS_MOD_SSL_POS = -1;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    BuiltWithTechIndicators source;

    @Autowired
    BuiltWithMostRecent baseSource;

    @Autowired
    private BuiltWithTechIndicatorsService service;

    @Autowired
    private SourceColumnEntityMgr sourceColumnEntityMgr;

    @Test(groups = "functional")
    public void testTransformation() {
        readBitPositions();

        uploadBaseAvro(baseSource, baseSourceVersion);
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected TransformationService<BasicTransformationConfiguration> getTransformationService() {
        return service;
    }

    @Override
    protected Source getSource() {
        return source;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(source.getBaseSources()[0], baseSourceVersion).toString();
    }

    @Override
    protected BasicTransformationConfiguration createTransformationConfiguration() {
        BasicTransformationConfiguration configuration = new BasicTransformationConfiguration();
        configuration.setVersion(targetVersion);
        return configuration;
    }

    @Override
    protected String getPathForResult() {
        return hdfsPathBuilder.constructSnapshotDir(source, targetVersion).toString();
    }

    @Override
    public void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        int recordsToCheck = 100;
        int pos = 0;
        while (pos++ < recordsToCheck && records.hasNext()) {
            GenericRecord record = records.next();
            String domain = record.get("Domain").toString();
            try {
                boolean[] bits = BitCodecUtils.decode(record.get(TECH_INDICATORS).toString(),
                        new int[]{HAS_UNIX_POS, HAS_MOD_SSL_POS});
                if ("sandisland.com".equals(domain)) {
                    Assert.assertTrue(bits[0]);
                    Assert.assertTrue(bits[1]);
                }
            } catch (IOException e) {
                System.out.println(record);
                throw new RuntimeException(e);
            }
        }
    }

    private void readBitPositions() {
        List<SourceColumn> columns = sourceColumnEntityMgr.getSourceColumns(source.getSourceName());
        for (SourceColumn column : columns) {
            String columnName = column.getColumnName();
            if (TECH_UNIX.equals(columnName)) {
                HAS_UNIX_POS = parseBitPos(column.getArguments());
            } else if (TECH_MOD_SSL.equals(columnName)) {
                HAS_MOD_SSL_POS = parseBitPos(column.getArguments());
            }
            if (Collections.min(Arrays.asList(HAS_UNIX_POS, HAS_MOD_SSL_POS)) > -1) {
                break;
            }
        }
    }

    private int parseBitPos(String arguments) {
        try {
            JsonNode jsonNode = objectMapper.readTree(arguments);
            return jsonNode.get("BitPosition").asInt();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
