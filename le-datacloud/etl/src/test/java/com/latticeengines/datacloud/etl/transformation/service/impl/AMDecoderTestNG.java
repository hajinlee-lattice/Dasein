package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.entitymgr.SourceAttributeEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.AMDecoder;
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AMDecoderConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;

public class AMDecoderTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(AMDecoderTestNG.class);

    private static final String VERSION = "2017-09-29_00-00-00_UTC";
    private static final String DATA_CLOUD_VERSION = "2.0.6";
    private static final String TESTAM_DECODED_SOURCE = "AccountMasterTestDecoded";
    private static final String FULLAM_DECODED_SOURCE = "AccountMasterDecoded";
    private GeneralSource testAM = new GeneralSource("AccountMasterTest");
    private GeneralSource fullAM = new GeneralSource("AccountMaster");
    private GeneralSource testAMDecoded = new GeneralSource(TESTAM_DECODED_SOURCE);
    private GeneralSource fullAMDecoded = new GeneralSource(FULLAM_DECODED_SOURCE);
    private GeneralSource source = fullAMDecoded;

    @Inject
    private SourceAttributeEntityMgr srcAttrEntityMgr;

    private String[] retainedAttributes = {"LatticeID", "LDC_Duns", "LDC_Domain", "LDC_Name", "LDC_State", "LDC_Country"};
    private String[] decodedAttributes = {
            "BmbrSurge_3DPrinting_CompScore",
            "BmbrSurge_Accounting_Intent",
            "BmbrSurge_Bulldozers_BuckScore",
            "BmbrSurge_Cache_BuckScore",
            "BmbrSurge_DNASequencing_Intent",
            "BmbrSurge_Fidelity_CompScore",
            "TechIndicator_100TB",
            "TechIndicator_24Log",
            "TechIndicator_Aarki",
            "TechIndicator_BigCartel",
            "TechIndicator_Symantec",
            "TechIndicator_Tibco",
    };

    private String[] encodedAttributes = {"BmbrSurge_Intent", "BmbrSurge_CompositeScore", "BmbrSurge_BucketCode",
            "BuiltWith_TechIndicators", "HGData_SegmentTechIndicators", "HGData_SupplierTechIndicators"};

    private Object[][] inputData = new Object[][] {
            {71L, "079893664", "yiayias.com", "P B & J Restaurants, Inc.", "KANSAS", "USA",
                    "EAABUQAAAABAEAQAACMUFAFEQEUBBAFEQEAQUAAQBAQAEAAUFQABAAAEAAAAAEAQDBEABQQQAAAAAEAAAJAAAGABQANUBAABAAUEAQDEgABQwBAQBQABRAQBVAEEEBAAAAQBBEAAABQUAAAAAEEBUCAQAEBRBBAEBQBhABQgBEDAAAAAAARAAAAAABUEEgQAQCAQEAFRQEABQERAAAUEABMBAAAQgBFEAEERAAAFAEYQADkAEQAABABWAQUAEBAAABgEBBAQAAAABEAAABAABAUAEAAUxABVVgAQgABQNCAEAAIAAACAABEYBRABQVQFAgAAQIAUQgAQBRQAUAwBABAAUEBLAAAkBEAAEAUBIBVQQNAcEAgBAAAAAAAAAMBEEFAEAQAAAEAQEAEBAQAFQEEQEABBEEQXAIBBQBEQQNMCAwEEVAAACUQSUQARAABBDgUAAEAGCDQAEAgSEUETEBhMVAAAQI0BEAQQFBEBASAIAUAEhEURBQEEBAEBkJFARQQAAAAAAAAABAAAQBAEQABADQUGAAAMAQAQBABAQQAQQBEASFQEFEBQBBARABWQAcFFAkREAAMBHEQQAARGAAQZqANQgFYCCABRAYQBAAAAAAQUFFARFQAQAAEBQAAAAFAAAAEAAAABQAIAAEBBAAAAQBAAAEFAAAQBEAAABAUlBRAAUQEEAAAAMAAQAAUAQREEBAAAAAQAANQRAAAAAACAUUUFABAQAAAAAQAAIEQQRAUIUAEhFUAEAAARAAQAQFEFAAAABQEEAMEAAAEAQABBAUAAAEAEAEAUEgAQEAAABRAEEUAABBEEAQUAAQAAAUBAAEAAARAAEAAEAAAACAAAEQEBQABQAFEAEAAABQQAABAABUADBRBQMAQAAQARAO1ABFQAADAAREAQABVAUAAAAAAEAAAAUFUBABACAMAAAAQQQAEQAUAAFBQEAgAUQWQQBFEUAAAAA0AAEQQAABEEBEEQBYAEABAAQAAAAAQAAYABABVRBAEAQQIAAEQQBAABAwBAMBAAABBAABAAACQCABBBBEAQTAAEBAQUAEABAAAQFREAFEAABQAEARAFAQAAAEVAEATABEAABRDAQAAUFBEBBBQQAAEBBQEQABAAAAARFAQEAECAAAD0AQDQAAARQAAFwAQAIQAATBAAAABEAQ",
                    "AAC1AAAAAAC4AAAAmACytwAAAAAAAAAAAAAAAAAAAAAAAACqAACaAACzAAAAAAAAAAAAAMcAwQAAqLQAAJqtALYAAAAArQCUAAAAspmVAK6gAAAAAKUAAKkAAAAApQCZAAAAuwAAAJYAAKUAAACqrwAAAAAAAK0AAJMAAACTAAAAAAAAAACeAAAAAAAAi7YAqJmcAAAAAACsAAAAAAAAAAAAAAAAowAAAAAAAAAAAAAAAAAAAAAAAAAAAKkAAI8AAMYAAJ0AtgAAAAAAs68AAAC6AAAAAI8AAAAAAAAAAAAAAAAAAAAAAAAAALIAAAAAAAAAAAAApb0AAAAAAAAAAAAAvqKwAAAAAAAAocoAAAAAtq-gALoAAAAAAACvAAAAAAAAAJaoAAAArQAAuQAAAAAAAAAAmwDHAAAAwgAAAAAAAI2lAAAAxgAAtAAAAKoAr6QAAAAAAACsAAAAAKMApQClAACuAAAAAKScrLQAAAAAkAAAAACrAAAAuQAAAAAAAAAAAACmAACfAAAAALcAAAAAAJ8AAAAAAAAAAAChuQAAs7EAAAAAAAAAAAAAAAAAAAAAAK4AALStAAAAAACrsgAAvQAAALYAAAAAAAAAAKelAKqpAK8AAAAAqgAApAAAqacAAAAAAACKAL63AAAAAACoqgAAAMMAAJQAAAAAAK4AAADEAAAAAAAAAAAAAAAAAAAAAACfAAAAAACyAAAAAAAAAAAAAAAAAAAAALWtswAAqwAAwAC1AACmAAAAAAAAAAAAtAAAvwAAAK0AAACuALYAAAClAIiuAAAArgAAAK2ZAAAAAAAAqgCyAK0AAAC6AAAAAJqcAAAAngAAAAAAAMoApwCsAAAAAAAAAAAAAAAAAK0AAAAAvJcAtwAAlACtAAAAALYAAKmPAJwAAAAAAAAAAACZrQAAAAAAAMCQALAAAKUAAAAAALXCyAAAAAAAkwCXAAAAAAAAAAAAAKsAAAAAAAC-l7urqwAAAK2tAAAAAAAAAACuAAAAtQAAAAAAAAAAAADBkwAArQAAALYAAAAAtgAAALoAAAAAAAAAAAAAAAAAALYAAAAAAK0AAAAAAAAAAAAAmAAAAAAAAKYAALWnAAAAAAAAAACRAAAAAAAAnbEAAJEAzgAAAACpq6SmvLCoogAAAAAAALsAAAAAwQAAAAAAALauAKvMAAAAvAAArwAAAAAAAL4AAAAAAAAAAAAAAAAAAAAAAADCAAAAAJ0AkAAAvacAq4cAAAAAtgCUAAAAuQAAmgC6kqSLowAAvgAAAAAAAAAAAAAAAAAAkAAAAL4AmqsAwAAArAAAAAAAALUAorgAAACctAAAAAAAAAC2pwDRAACOAAAAAAAAAAAAqAAAAAAAAAChrQAAAKnFwwCcAAAAAAAAAAAAqr8AAJYAAAAAALIAAAAAAACtAKWsAACuAAAAAAC-AK6wuAAAAKSdAAAAmwAAr8gAxbEAAACvAADBAACyAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAANQAugCSAAC1AAAAoqYAtwAAnQAAAAAAAAAAAAAAAAAAAAAAALcAAKsAAACwAKIAAACkAAAAtAAAAAAAAACvkQAAAAAAqJUAAJ4AALAAAACvAAAAAACwAACoAAC2AACLAK_GiqwAAAAAAAAAAMKUAAClAAAAr6AAtwAAALQAAAAAq8cAssS8AAAAxAAAALcAAAAAuQAAAJ2ojAAAAAAAAAAAl70AAAClALPBAJoAqAC1tgAAAAC5AK4AAAAAAAAAAAC3AACrvccAALC4AAAAAAAAAAAAAAAAALa9swAAAMIAAACxygAAAAAAAACSAADDAAC9AKoAtACvAKUAAKzFAJIAAACJAADCmwAAygCwALaiuQAAAAAAAAAAAAAAp5nIAL-aAAAAAACWAACZAAAAALEAAK25AK4AkACuAAAAqgAAAAAAwAAAvgAAtQAAAAAAALkAogAAALcAvbSjAI-sAJUAuZoAALAAAAAAnQAAALIAAJYAAAC1AAAAAACvv6wAocEAAACbpbgAmQCjAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAK8AAAAAAAAAAAAAAAAArgAArQAAmQAAAAAAsAAAAAAAAACmtsUAALW1AADAsQAAAAAAAAAAAAAA0AAAuwAAAAAAAAAAALIAAKwAAAAAAAAAAACusAAArgAAAAAAAKIAAAAAqJsAsgAAAAAAAL8AmgCtqLQArQAAALivAAAAALMAAKu2AK0AAAAAmQCuALAAAAAAAKuuqwAAAJC9owAAAKwAAMm4kQCuwAAAAACwALQArACxAAAAAMsAAACkAAAAAMSaAACfALYAAKoAAAAAAACgAAC_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",
                    "IAADsgAAAABAMAgAABI8PAOIgMUBDAOIQIAQ0AAwDAQAIAA4OwADAAAEAAAAAMAwDDEADggwAAAAAMAAAKAAAJADQAGcCAADAAcMAwCEwACgQDAgCwADTAgDnAMIMCAAAAQBDIAAACQ8AAAAAMMD4BAwAMCTBCAEDgBhABQQCMDAAAAAAAjAAAAAAD8EIwwAwCAwEAHjwMACQExAAAUIACIDAAAwgCPIAEMiAAANAE8wADoAMgAADADWAw8AMDAAABQEDDAwAAAADMAAABAADAUAMAA0xACVWgAQwADgPCAMAAMAAADAACEoBzACgpwKAwAAQIAcQwAQDhwA8AwCABAAoEDFAAA4BEAAMA8CMD6QgPA4MAwBAAAAAAAAAMBEMNAMAwAAAEAwIAIDAwAPwIMwMABBMMg3AIDBwDEwwPMCAgIErAAAC8gjYgAyAACCCw8AAMAJCDwAEAwTM8MzEBiMXAAAwIkDEAwwPBMBASAEAsAISMcjDwEICAIDcJNASQwAAAAAAAAACAAAwDAMwACABwsNAAAMAQAgBADAwgAQgDMARJwMPMDwCDASADvQAoOFA0yMAAMDOMgwAAzKAAQ6bAOgQGsDDADxAkgBAAAAAAgsHPAiNQAwAAMCQAAAAGAAAAMAAAACgAMAAICBAAAAwDAAAMFAAAwDIAAABA8dBxAA0wMEAAAAIAAgAAoAwxEMDAAAAAQAAOwSAAAAAADAcY8KADAQAAAAAwAAMIQQyA4EsAIxL8AMAAAiAAQAwPENAAAABwMMAIMAAAIAwADBA4AAAEAIAEAkEwAwIAAABRAMM4AACDMIAgsAAgAAA8CAAMAAAyAAMAAMAAAADAAAIwMDgADwAKIAEAAACwwAADAAB0ADBzDQMAwAAwAiAPnADOwAABAAxIAwADnA4AAAAAAMAAAA8LoBADABAMAAAAwwQAIgA4AAPDwMAgA8w2QwDPI4AAAAA4AAIgwAADMMBEIwD0AIACAAwAAAAAgAAUADAC-TCAIAwwMAAIgwBAABAwDAMDAAADDAACAAACQDACBCDIAwzAAECAwsAEADAAAQHhEALMAADgAMAzAHAQAAAIrAMAzADEAADiDAgAAoODMDCDwwAAMDCQMQADAAAAAzKAgMAMCAAABkAQDwAAAzwAALwAgAMwAASBAAAADMAQ",
                    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEA",
                    null, null},
            {863L, "365079685", "talktalkbusiness.co.uk", "VIDEO NETWORKS LIMITED", "LONDON", "UNITED KINGDOM",
                    null, null, null,
                    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAIAAAAAAAAAAAAAAAAAAAAAACAAABAAAAAgIAAAAAAAAAOAAAAwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAgAAAQABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAAAAgAACEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAgAAAAAAAAAAAAMAAAAAAAAAQAAAAQAAAAAAAAAAAACwIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAgAABAAAAAAAAAAAAAAAAAAAAAAAAABRAQQAAAEAAAAAAAAAAAAAAAAIAAAAAAACAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAQABAgE0kAMAABBQAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAZAAAAAAAAAAAAABAAwgEAAAAAAAAAAAgAAAAAAAAAAAgAAAAAAAAAAAgAAAAEAAQAAAAAAAAABAAAAAAAQAAAEUA5QgBACAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAQAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAABAAAAAAAAgAAAAAACAAAAAAAIAAAAAAAAIAAAAAEAAAMAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAwCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAgAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAABAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAQAAAAAMAAAAAAAAAAAAAAAAAAAAAAAAAAfAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIIAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAIAQAAAAAAAICAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAABAEQAAAIAAAAAAAAAAAAAAAAAQABAAAAAAAAMAAAAAAAAEBAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgCAAAAIAAAAAAAAAAAAAAEDAACAAAAAAAAIEABAEAAAAAAI",
                    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQ",
                    "AAAAAAAAAAAAAACA"}
    };

    private Object[][] expectedData = new Object[][] {
            {71L, "079893664", "yiayias.com", "P B & J Restaurants, Inc.", "KANSAS", "USA",
                    null, null, "A", null, "Moderate", 32, "No", "No", null, null, null, null},
            {863L, "365079685", "talktalkbusiness.co.uk", "VIDEO NETWORKS LIMITED", "LONDON", "UNITED KINGDOM",
                    null, null, null, null, null, null, "No", "No", "No", "No", "No", "No"}
    };

    @Test(groups = "pipeline1")
    public void testTransformation() {
        prepareData();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmIntermediateSource(testAMDecoded, null);
        confirmIntermediateSource(fullAMDecoded, null);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("AMDecoder");
            configuration.setVersion(targetVersion);

            // Decode manually prepared am
            TransformationStepConfig step0 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<>();
            baseSources.add(testAM.getSourceName());
            step0.setBaseSources(baseSources);
            step0.setTransformer(AMDecoder.TRANSFORMER_NAME);
            step0.setTargetSource(testAMDecoded.getSourceName());
            step0.setConfiguration(getAMDecoderConfig());

            // Decode sampled full am
            TransformationStepConfig step1 = new TransformationStepConfig();
            baseSources = new ArrayList<>();
            baseSources.add(fullAM.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer(AMDecoder.TRANSFORMER_NAME);
            step1.setTargetSource(fullAMDecoded.getSourceName());
            step1.setConfiguration(setDataFlowEngine("{}", "TEZ"));

            // -----------
            List<TransformationStepConfig> steps = new ArrayList<>();
            steps.add(step0);
            steps.add(step1);

            // -----------
            configuration.setSteps(steps);

            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String getAMDecoderConfig() throws JsonProcessingException {
        AMDecoderConfig conf = new AMDecoderConfig();
        conf.setRetainFields(retainedAttributes);
        conf.setDecodeFields(decodedAttributes);
        return JsonUtils.serialize(conf);
    }

    private void prepareData() {
        // Manually prepared testing am
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(retainedAttributes[0], Long.class));
        for (int i = 1; i < retainedAttributes.length; i++) {
            columns.add(Pair.of(retainedAttributes[i], String.class));
        }
        for (String toDecode : encodedAttributes) {
            columns.add(Pair.of(toDecode, String.class));
        }
        uploadBaseSourceData(testAM.getSourceName(), VERSION, columns, inputData);
        prepareSchemaFile(testAM, VERSION);
        // Sampled full am
        uploadBaseSourceFile(fullAM.getSourceName(), "AccountMaster206", VERSION);
        prepareSchemaFile(fullAM, VERSION);

    }

    private void prepareSchemaFile(Source source, String version) {
        try {
            String avroDir = hdfsPathBuilder.constructTransformationSourceDir(source, version).toString();
            List<String> avroFiles = HdfsUtils.getFilesByGlob(yarnConfiguration, avroDir + "/*.avro");
            Schema schema = AvroUtils.getSchema(yarnConfiguration, new Path(avroFiles.get(0)));
            schema.addProp(AMDecoder.DATA_CLOUD_VERSION_NAME, DATA_CLOUD_VERSION);

            String avscPath = hdfsPathBuilder.constructSchemaFile(source.getSourceName(), version).toString();
            HdfsUtils.writeToFile(yarnConfiguration, avscPath, schema.toString());
        } catch (IOException e) {
            throw new RuntimeException("Fail to create schema file for AccountMaster ", e);
        }
    }

    @Override
    protected void verifyIntermediateResult(String source, String version, Iterator<GenericRecord> records) {
        switch (source) {
        case TESTAM_DECODED_SOURCE:
            verifyTestAMDecoded(source, version, records);
            break;
        case FULLAM_DECODED_SOURCE:
            verifyFullAMDecoded(source, version, records);
        default:
            break;
        }
    }

    private void verifyTestAMDecoded(String source, String version, Iterator<GenericRecord> records) {
        log.info("Start to verify source {} @{}", source, version);
        Map<Long, Object[]> expectedMap = new HashMap<>();
        for (Object[] data : expectedData) {
            expectedMap.put((Long) data[0], data);
        }
        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            Object[] expectedResult = expectedMap.get(record.get(retainedAttributes[0]));
            Assert.assertTrue(isObjEquals(record.get(retainedAttributes[0]), expectedResult[0]));
            Assert.assertTrue(isObjEquals(record.get(retainedAttributes[1]), expectedResult[1]));
            Assert.assertTrue(isObjEquals(record.get(retainedAttributes[2]), expectedResult[2]));
            Assert.assertTrue(isObjEquals(record.get(retainedAttributes[3]), expectedResult[3]));
            Assert.assertTrue(isObjEquals(record.get(retainedAttributes[4]), expectedResult[4]));
            Assert.assertTrue(isObjEquals(record.get(retainedAttributes[5]), expectedResult[5]));
            Assert.assertTrue(isObjEquals(record.get(decodedAttributes[0]), expectedResult[6]));
            Assert.assertTrue(isObjEquals(record.get(decodedAttributes[1]), expectedResult[7]));
            Assert.assertTrue(isObjEquals(record.get(decodedAttributes[2]), expectedResult[8]));
            Assert.assertTrue(isObjEquals(record.get(decodedAttributes[3]), expectedResult[9]));
            Assert.assertTrue(isObjEquals(record.get(decodedAttributes[4]), expectedResult[10]));
            Assert.assertTrue(isObjEquals(record.get(decodedAttributes[5]), expectedResult[11]));
            Assert.assertTrue(isObjEquals(record.get(decodedAttributes[6]), expectedResult[12]));
            Assert.assertTrue(isObjEquals(record.get(decodedAttributes[7]), expectedResult[13]));
            Assert.assertTrue(isObjEquals(record.get(decodedAttributes[8]), expectedResult[14]));
            Assert.assertTrue(isObjEquals(record.get(decodedAttributes[9]), expectedResult[15]));
            Assert.assertTrue(isObjEquals(record.get(decodedAttributes[10]), expectedResult[16]));
            Assert.assertTrue(isObjEquals(record.get(decodedAttributes[11]), expectedResult[17]));
            rowNum++;
        }
        Assert.assertEquals(rowNum, 2);
    }

    private void verifyFullAMDecoded(String source, String version, Iterator<GenericRecord> records) {
        log.info("Start to verify source {} @{}", source, version);
        List<SourceAttribute> srcAttrs = srcAttrEntityMgr.getAttributes("AccountMasterDecoded", "DECODE",
                AMDecoder.TRANSFORMER_NAME, DATA_CLOUD_VERSION, false);
        Set<String> decodedAttrs = new HashSet<>(
                srcAttrs.stream().map(SourceAttribute::getAttribute).collect(Collectors.toList()));
        int cols = 0;
        boolean decodedAttrPopulated = false;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            for (Schema.Field field : record.getSchema().getFields()) {
                if (decodedAttrs.contains(field.name()) && record.get(field.name()) != null) {
                    decodedAttrPopulated = true;
                }
            }
            if (cols == 0) {
                cols = record.getSchema().getFields().size();
            }
        }
        Assert.assertTrue(decodedAttrPopulated);
        log.info("Total decoded column number: {}", cols);
        Assert.assertTrue(cols >= 25000);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
    }

}
