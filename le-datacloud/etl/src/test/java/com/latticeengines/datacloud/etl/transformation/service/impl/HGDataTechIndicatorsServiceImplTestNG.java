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
import com.latticeengines.datacloud.core.source.impl.HGDataClean;
import com.latticeengines.datacloud.core.source.impl.HGDataTechIndicators;
import com.latticeengines.datacloud.etl.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;

public class HGDataTechIndicatorsServiceImplTestNG
        extends TransformationServiceImplTestNGBase<BasicTransformationConfiguration> {

    private static final Log log = LogFactory.getLog(HGDataTechIndicatorsServiceImplTestNG.class);

    private final String SEGMENT_INDICATORS = "SegmentTechIndicators";
    private final String SUPPLIER_INDICATORS = "SupplierTechIndicators";

    private final String TECH_VMWARE = "TechIndicator_VMware";
    private final String TECH_VSPHERE = "TechIndicator_VMwarevSphere";

    private final String TECH_IBM = "TechIndicator_IBM";
    private final String TECH_COGNOS = "TechIndicator_CognosImpromptu";

    private int HAS_VMWARE_POS = -1;
    private int HAS_VSPHERE_POS = -1;
    private int HAS_IBM_POS = -1;
    private int HAS_COGNOS_POS = -1;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    HGDataTechIndicators source;

    @Autowired
    HGDataClean baseSource;

    @Autowired
    private HGDataTechIndicatorsService service;

    @Autowired
    private SourceColumnEntityMgr sourceColumnEntityMgr;

    @Test(groups = "pipeline2")
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
    TransformationService<BasicTransformationConfiguration> getTransformationService() {
        return service;
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
    public void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        int recordsToCheck = 100;
        int pos = 0;
        while (pos++ < recordsToCheck && records.hasNext()) {
            GenericRecord record = records.next();
            String domain = record.get("Domain").toString();
            try {
                boolean[] bits = BitCodecUtils.decode(record.get(SEGMENT_INDICATORS).toString(),
                        new int[]{HAS_VSPHERE_POS, HAS_COGNOS_POS});
                boolean[] bits2 = BitCodecUtils.decode(record.get(SUPPLIER_INDICATORS).toString(),
                        new int[]{HAS_VMWARE_POS, HAS_IBM_POS});
                if ("avon.com".equals(domain)) {
                    Assert.assertTrue(bits[1]);
                    Assert.assertTrue(bits2[1]);
                }
                if ("arcelormittal.com".equals(domain)) {
                    Assert.assertTrue(bits[0]);
                    Assert.assertTrue(bits2[0]);
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
            if (TECH_VMWARE.equals(columnName)) {
                HAS_VMWARE_POS = parseBitPos(column.getArguments());
            } else if (TECH_VSPHERE.equals(columnName)) {
                HAS_VSPHERE_POS = parseBitPos(column.getArguments());
            } else if (TECH_IBM.equals(columnName)) {
                HAS_IBM_POS = parseBitPos(column.getArguments());
            } else if (TECH_COGNOS.equals(columnName)) {
                HAS_COGNOS_POS = parseBitPos(column.getArguments());
            }
            if (Collections.min(Arrays.asList(HAS_VMWARE_POS, HAS_VSPHERE_POS, HAS_IBM_POS, HAS_COGNOS_POS)) > -1) {
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
