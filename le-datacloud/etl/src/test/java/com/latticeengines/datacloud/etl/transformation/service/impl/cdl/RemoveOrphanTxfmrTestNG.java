package com.latticeengines.datacloud.etl.transformation.service.impl.cdl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.cdl.RemoveOrphanTxfmr;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.spark.cdl.RemoveOrphanConfig;

public class RemoveOrphanTxfmrTestNG extends PipelineTransformationTestNGBase {

    private final GeneralSource parentSrc = new GeneralSource("Parent");
    private final GeneralSource childSrc = new GeneralSource("Child");

    private final String ParentId = "ParentId";

    @Override
    protected String getTargetSourceName() {
        return "Offspring";
    }

    @Test(groups = "functional")
    public void testTransformation() {
        prepareAccountSrc();
        prepareContactSrc();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("DnBClean");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<>();
            baseSources.add(childSrc.getSourceName());
            baseSources.add(parentSrc.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer(RemoveOrphanTxfmr.TRANSFORMER_NAME);
            step1.setTargetSource(getTargetSourceName());
            RemoveOrphanConfig jobConfig = new RemoveOrphanConfig();
            jobConfig.setParentId(ParentId);
            step1.setConfiguration(JsonUtils.serialize(jobConfig));

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(step1);

        // -----------
        configuration.setSteps(steps);

        return configuration;
    }

    private void prepareAccountSrc() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(ParentId, String.class));
        columns.add(Pair.of("AccountAttr", String.class));
        columns.add(Pair.of("Attr", String.class));
        uploadBaseSourceData(parentSrc.getSourceName(), baseSourceVersion, columns, new Object[][] { //
                { "Account1", "a", "a" }, //
                { "Account2", "b", "a" }, //
                { "Account9", "b", "a" }, //
        });
    }

    private void prepareContactSrc() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("ChildId", String.class));
        columns.add(Pair.of(ParentId, String.class));
        columns.add(Pair.of("ContactAttr", String.class));
        columns.add(Pair.of("Attr", String.class));
        uploadBaseSourceData(childSrc.getSourceName(), baseSourceVersion, columns, new Object[][] { //
                { "Contact1", "Account1", "a", "b" }, //
                { "Contact2", "Account1", "b", "b" }, //
                { "Contact3", "Account2", "c", "b" }, //
                { "Contact4", "Account2", "d", "b" }, //
                { "Contact5", "Account3", "e", "b" }, //
        });
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int count = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            Assert.assertEquals(record.getSchema().getFields().size(), 4);
            Assert.assertEquals(record.get("Attr").toString(), "b");
            Assert.assertNotEquals(record.get(ParentId).toString(), "Account3");
            Assert.assertNotEquals(record.get(ParentId).toString(), "Account9");
            count++;
        }
        Assert.assertEquals(count, 4);
    }

}
