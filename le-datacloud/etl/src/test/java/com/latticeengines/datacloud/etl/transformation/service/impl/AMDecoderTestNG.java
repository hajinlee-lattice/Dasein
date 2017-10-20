package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.AMDecoder;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AMDecoderConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AMDecoderTestNG extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(AMDecoderTestNG.class);
    private GeneralSource baseSource = new GeneralSource("AccountMaster");
    private GeneralSource source = new GeneralSource("AccountMasterDecoded");
    private static final String VERSION = "2017-09-29_00-00-00_UTC";
    private static final String DATA_CLOUD_VERSION = "2.0.6";

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
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("AMDecoder");
            configuration.setVersion(targetVersion);

            TransformationStepConfig singleStep = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<>();
            baseSources.add(baseSource.getSourceName());
            singleStep.setBaseSources(baseSources);
            singleStep.setTransformer(AMDecoder.TRANSFORMER_NAME);
            singleStep.setTargetSource(source.getSourceName());
            String confParamStr1 = getAMDecoderConfig();
            singleStep.setConfiguration(confParamStr1);

            // -----------
            List<TransformationStepConfig> steps = new ArrayList<>();
            steps.add(singleStep);

            // -----------
            configuration.setSteps(steps);

            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        Map<Long, Object[]> expectedMap = new HashMap<>();
        for (Object[] data : expectedData) {
            expectedMap.put((Long) data[0], data);
        }
        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            Object[] expectedResult = expectedMap.get((Long) record.get(retainedAttributes[0]));
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

    private String getAMDecoderConfig() throws JsonProcessingException {
        AMDecoderConfig conf = new AMDecoderConfig();
        conf.setRetainFields(retainedAttributes);
        conf.setDecodeFields(decodedAttributes);
        return JsonUtils.serialize(conf);
    }

    private void prepareData() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(retainedAttributes[0], Long.class));
        for (int i = 1; i < retainedAttributes.length; i++) {
            columns.add(Pair.of(retainedAttributes[i], String.class));
        }
        for (String toDecode : encodedAttributes) {
            columns.add(Pair.of(toDecode, String.class));
        }
        uploadBaseSourceData(baseSource.getSourceName(), VERSION, columns, inputData);

        try {
            String avroDir = hdfsPathBuilder.constructTransformationSourceDir(baseSource, VERSION).toString();
            List<String> src2Files = HdfsUtils.getFilesByGlob(yarnConfiguration, avroDir + "/*.avro");
            Schema schema = AvroUtils.getSchema(yarnConfiguration, new Path(src2Files.get(0)));
            schema.addProp(AMDecoder.DATA_CLOUD_VERSION_NAME, DATA_CLOUD_VERSION);

            String avscPath = hdfsPathBuilder.constructSchemaFile(baseSource.getSourceName(), VERSION).toString();
            HdfsUtils.writeToFile(yarnConfiguration, avscPath, schema.toString());
        } catch (IOException e) {
            throw new RuntimeException("Fail to create schema file for AccountMaster ", e);
        }
    }
}
