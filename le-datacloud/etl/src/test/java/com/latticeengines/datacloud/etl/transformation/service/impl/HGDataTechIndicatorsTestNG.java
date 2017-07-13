package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.BitCodecUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.HGDataTechIndicatorsFlow;
import com.latticeengines.datacloud.etl.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TechIndicatorsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class HGDataTechIndicatorsTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(HGDataTechIndicatorsTestNG.class);

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

    private final ObjectMapper om = new ObjectMapper();

    GeneralSource source = new GeneralSource("HGDataTechIndicators");
    GeneralSource baseSource = new GeneralSource("HGDataClean");

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
    protected TransformationService<PipelineTransformationConfiguration> getTransformationService() {
        return pipelineTransformationService;
    }

    @Override
    protected Source getSource() {
        return source;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("HGDataTechIndicators");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add(baseSource.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer(HGDataTechIndicatorsFlow.TRANSFORMER_NAME);
            step1.setTargetSource(source.getSourceName());
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
        TechIndicatorsConfig config = new TechIndicatorsConfig();
        String[] groupByFields = { "Domain" };
        config.setGroupByFields(groupByFields);
        config.setTimestampField("Timestamp");
        return om.writeValueAsString(config);
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
            JsonNode jsonNode = om.readTree(arguments);
            return jsonNode.get("BitPosition").asInt();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
