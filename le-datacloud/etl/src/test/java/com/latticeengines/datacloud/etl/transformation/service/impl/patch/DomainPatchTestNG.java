package com.latticeengines.datacloud.etl.transformation.service.impl.patch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.patch.DomainPatch;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.datacloud.etl.transformation.service.impl.dunsredirect.DunsGuideBookDepivotTestNG;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.DomainPatchConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class DomainPatchTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(DunsGuideBookDepivotTestNG.class);

    private GeneralSource domainPatch = new GeneralSource("DomainPatchBook");
    private GeneralSource ams = new GeneralSource("AccountMasterSeedMerged");
    private GeneralSource amsPatched = new GeneralSource("AccountMasterSeedMergedPatched");
    private GeneralSource source = amsPatched;

    private static final String DOM_SRC = "Patch";
    private static final String VAL_Y = DataCloudConstants.ATTR_VAL_Y;
    private static final String VAL_N = DataCloudConstants.ATTR_VAL_N;

    @Test(groups = "pipeline1")
    public void testTransformation() {
        prepareDomainPatchBook();
        prepareAMS();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("DomainPatch");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step0 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>();
        baseSources.add(domainPatch.getSourceName());
        baseSources.add(ams.getSourceName());
        step0.setBaseSources(baseSources);
        step0.setTransformer(DomainPatch.TRANSFORMER_NAME);
        step0.setTargetSource(source.getSourceName());
        step0.setConfiguration(getDomainPatchConfig());

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step0);

        // -----------
        configuration.setSteps(steps);
        return configuration;
    }

    private String getDomainPatchConfig() {
        DomainPatchConfig config = new DomainPatchConfig();
        config.setDomainSource(DOM_SRC);
        return JsonUtils.serialize(config);
    }

    // Schema: DUNS, PatchItems, Cleanup
    private Object[][] domainPatchBookData = new Object[][] { //
            /* Case 01: input duns does not exist in AMSeed, do nothing */
            { "DUNS_NOTEXIST", "{\"LDC_Domain\":\"dom1.com\"}", false }, //
            { "DUNS_NOTEXIST", "{\"LDC_Domain\":\"dom1_1.com\"}", true }, //

            /* Case 03: input duns in AMSeed is duns-only entry */
            // populate domain for duns-only entry
            { "DUNS3", "{\"LDC_Domain\":\"dom3.com\"}", false }, //
            // populate domain for duns-only entry, add more rows
            { "DUNS3_1", "{\"LDC_Domain\":\"dom3_1_1.com\"}", false }, //
            { "DUNS3_1", "{\"LDC_Domain\":\"dom3_1_2.com\"}", false }, //
            // populate domain for duns-only entry, add more rows, also try to
            // clean up domain but actually no action taken since there is no
            // domain for duns-only entry)
            { "DUNS3_2", "{\"LDC_Domain\":\"dom3_2_1.com\"}", false }, //
            { "DUNS3_2", "{\"LDC_Domain\":\"dom3_2_2.com\"}", false }, //
            { "DUNS3_2", "{\"LDC_Domain\":\"dom3_2_3.com\"}", true }, //
            // try to clean up domain but actually no action taken since there
            // is no domain for duns-only entry)
            { "DUNS3_3", "{\"LDC_Domain\":\"dom3_3.com\"}", true }, //

            /*
             * Case 04: input duns in AMSeed only has one row and has domain
             * populated
             */
            // Add more rows
            { "DUNS4", "{\"LDC_Domain\":\"dom4_1.com\"}", false }, //
            // Domain to add is same as existing domain, do nothing
            { "DUNS4_1", "{\"LDC_Domain\":\"dom4_1.com\"}", false }, //
            // Set domain to null
            { "DUNS4_2", "{\"LDC_Domain\":\"dom4_2.com\"}", true }, //
            // Domain to clean up does not exist, no action taken
            { "DUNS4_3", "{\"LDC_Domain\":\"dom4_3_1.com\"}", true }, //
            // Update domain - clean up existing domain by updating to
            // domain to add, also add more rows
            { "DUNS4_4", "{\"LDC_Domain\":\"dom4_4.com\"}", true }, //
            { "DUNS4_4", "{\"LDC_Domain\":\"dom4_4_1.com\"}", false }, //
            { "DUNS4_4", "{\"LDC_Domain\":\"dom4_4_2.com\"}", false }, //

            /*
             * Case 05: input duns in AMSeed has multiple rows and all of them
             * have domain populated
             */
            // Add more rows
            { "DUNS5_1", "{\"LDC_Domain\":\"dom5_1_3.com\"}", false }, //
            { "DUNS5_1", "{\"LDC_Domain\":\"dom5_1_4.com\"}", false }, //
            // Domain to add is same as existing domain, no action taken
            { "DUNS5_2", "{\"LDC_Domain\":\"dom5_2_1.com\"}", false }, //
            { "DUNS5_2", "{\"LDC_Domain\":\"dom5_2_2.com\"}", false }, //
            // Remove some rows
            { "DUNS5_3", "{\"LDC_Domain\":\"dom5_3_1.com\"}", true }, //
            { "DUNS5_3", "{\"LDC_Domain\":\"dom5_3_2.com\"}", true }, //
            // Duns has 2 domains, set one of domain to null, remove the other
            // row
            { "DUNS5_4", "{\"LDC_Domain\":\"dom5_4_1.com\"}", true }, //
            { "DUNS5_4", "{\"LDC_Domain\":\"dom5_4_2.com\"}", true }, //
            // Domain to clean up does not exist
            { "DUNS5_5", "{\"LDC_Domain\":\"dom5_4_3.com\"}", true }, //
            // Update domain - clean up existing domain by updating to domain to
            // add, also remove some rows
            { "DUNS5_6", "{\"LDC_Domain\":\"dom5_6_1.com\"}", true }, //
            { "DUNS5_6", "{\"LDC_Domain\":\"dom5_6_2.com\"}", true }, //
            { "DUNS5_6", "{\"LDC_Domain\":\"dom5_6_4.com\"}", false }, //

            /*
             * Case 06: input duns in AMSeed has multiple rows, one of them is
             * duns-only entry and others all have domain populated
             */
            // Remove all the rows with domain populated, leave duns-only
            // entry there
            { "DUNS6_1", "{\"LDC_Domain\":\"dom6_1_1.com\"}", true }, //
            { "DUNS6_1", "{\"LDC_Domain\":\"dom6_1_2.com\"}", true }, //
            // Populate duns-only entry with domain to add, also add some
            // rows and remove some rows
            { "DUNS6_2", "{\"LDC_Domain\":\"dom6_2_1.com\"}", true }, //
            { "DUNS6_2", "{\"LDC_Domain\":\"dom6_2_2.com\"}", true }, //
            { "DUNS6_2", "{\"LDC_Domain\":\"dom6_2_3.com\"}", false }, //
            { "DUNS6_2", "{\"LDC_Domain\":\"dom6_2_4.com\"}", false }, //
    };

    // Schema: DUNS, Domain, LE_IS_PRIMARY_DOMAIN,
    // LE_IS_PRIMARY_LOCATION, DomainSource, Name, SALES_VOLUME_US_DOLLARS
    private Object[][] amsData = new Object[][] { //
            /*
             * Case 02: Duns in AMSeed does not exist in DomainPatchBook. They
             * are not impacted by patch
             */
            { "DUNS2", "dom2.com", VAL_Y, VAL_N, "DnB", "Name2", 20000L }, //
            { "DUNS2", "dom2_1.com", VAL_Y, VAL_Y, "Orb", "Name2", 20000L }, //
            { "DUNS2_1", "dom2.com", VAL_N, VAL_Y, "HG", "Name2_1", 21000L }, //
            { "DUNS2_2", null, VAL_N, VAL_N, null, "Name2_2", 22000L }, //
            { null, "dom2.com", VAL_Y, VAL_N, "RTS", "NameNull", null }, //

            /* Case 03: input duns in AMSeed is duns-only entry */
            { "DUNS3", null, VAL_N, VAL_N, null, "Name3", 30000L }, //
            { "DUNS3_1", null, VAL_N, VAL_N, null, "Name3_1", 31000L }, //
            { "DUNS3_2", null, VAL_N, VAL_N, null, "Name3_2", 32000L }, //
            { "DUNS3_3", null, VAL_N, VAL_N, null, "Name3_3", 33000L }, //

            /*
             * Case 04: input duns in AMSeed only has one row and has domain
             * populated
             */
            { "DUNS4", "dom4.com", VAL_Y, VAL_Y, "DnB", "Name4", 40000L }, //
            { "DUNS4_1", "dom4_1.com", VAL_Y, VAL_Y, "DnB", "Name4_1", 41000L }, //
            { "DUNS4_2", "dom4_2.com", VAL_Y, VAL_Y, "DnB", "Name4_2", 42000L }, //
            { "DUNS4_3", "dom4_3.com", VAL_Y, VAL_Y, "DnB", "Name4_3", 43000L }, //
            { "DUNS4_4", "dom4_4.com", VAL_Y, VAL_Y, "DnB", "Name4_4", 44000L }, //

            /*
             * Case 05: input duns in AMSeed has multiple rows and all of them
             * have domain populated
             */
            { "DUNS5_1", "dom5_1_1.com", VAL_Y, VAL_Y, "DnB", "Name5_1", 51000L }, //
            { "DUNS5_1", "dom5_1_2.com", VAL_N, VAL_Y, "DnB", "Name5_1", 51000L }, //
            { "DUNS5_2", "dom5_2_1.com", VAL_Y, VAL_Y, "DnB", "Name5_2", 52000L }, //
            { "DUNS5_2", "dom5_2_2.com", VAL_N, VAL_Y, "DnB", "Name5_2", 52000L }, //
            { "DUNS5_3", "dom5_3_1.com", VAL_Y, VAL_Y, "DnB", "Name5_3", 53000L }, //
            { "DUNS5_3", "dom5_3_2.com", VAL_N, VAL_Y, "DnB", "Name5_3", 53000L }, //
            { "DUNS5_3", "dom5_3_3.com", VAL_N, VAL_Y, "DnB", "Name5_3", 53000L }, //
            { "DUNS5_4", "dom5_4_1.com", VAL_Y, VAL_Y, "DnB", "Name5_4", 54000L }, //
            { "DUNS5_4", "dom5_4_2.com", VAL_N, VAL_Y, "DnB", "Name5_4", 54000L }, //
            { "DUNS5_5", "dom5_5_1.com", VAL_Y, VAL_Y, "DnB", "Name5_5", 55000L }, //
            { "DUNS5_5", "dom5_5_2.com", VAL_N, VAL_Y, "DnB", "Name5_5", 55000L }, //
            { "DUNS5_6", "dom5_6_1.com", VAL_Y, VAL_Y, "DnB", "Name5_6", 56000L }, //
            { "DUNS5_6", "dom5_6_2.com", VAL_N, VAL_Y, "DnB", "Name5_6", 56000L }, //
            { "DUNS5_6", "dom5_6_3.com", VAL_N, VAL_Y, "DnB", "Name5_6", 56000L }, //

            /*
             * Case 06: input duns in AMSeed has multiple rows, one of them is
             * duns-only entry and others all have domain populated
             */
            { "DUNS6_1", "dom6_1_1.com", VAL_Y, VAL_Y, "DnB", "Name6_1", 61000L }, //
            { "DUNS6_1", "dom6_1_2.com", VAL_N, VAL_Y, "DnB", "Name6_1", 61000L }, //
            { "DUNS6_1", null, VAL_N, VAL_Y, "DnB", "Name6_1", 61000L }, //
            { "DUNS6_2", "dom6_2_1.com", VAL_Y, VAL_Y, "DnB", "Name6_2", 62000L }, //
            { "DUNS6_2", "dom6_2_2.com", VAL_N, VAL_Y, "DnB", "Name6_2", 62000L }, //
            { "DUNS6_2", null, VAL_N, VAL_Y, "DnB", "Name6_2", 62000L }, //
    };

    // Schema (same as AMSeed): DUNS, Domain, LE_IS_PRIMARY_DOMAIN,
    // LE_IS_PRIMARY_LOCATION, DomainSource, Name, SALES_VOLUME_US_DOLLARS
    private Object[][] expectedData = new Object[][] {
            /*
             * Case 02: Duns in AMSeed does not exist in DomainPatchBook. They
             * are not impacted by patch
             */
            { "DUNS2", "dom2.com", VAL_Y, VAL_N, "DnB", "Name2", 20000L }, //
            { "DUNS2", "dom2_1.com", VAL_Y, VAL_Y, "Orb", "Name2", 20000L }, //
            { "DUNS2_1", "dom2.com", VAL_N, VAL_Y, "HG", "Name2_1", 21000L }, //
            { "DUNS2_2", null, VAL_N, VAL_N, null, "Name2_2", 22000L }, //
            { null, "dom2.com", VAL_Y, VAL_N, "RTS", "NameNull", null }, //

            /* Case 03: input duns in AMSeed is duns-only entry */
            // populate domain for duns-only entry
            { "DUNS3", "dom3.com", VAL_N, VAL_N, DOM_SRC, "Name3", 30000L }, //
            // populate domain for duns-only entry, add more rows
            { "DUNS3_1", "dom3_1_1.com", VAL_N, VAL_N, DOM_SRC, "Name3_1", 31000L }, //
            { "DUNS3_1", "dom3_1_2.com", VAL_N, VAL_N, DOM_SRC, "Name3_1", 31000L }, //
            // populate domain for duns-only entry, add more rows, also try to
            // clean up domain but actually no action taken since there is no
            // domain for duns-only entry)
            { "DUNS3_2", "dom3_2_1.com", VAL_N, VAL_N, DOM_SRC, "Name3_2", 32000L }, //
            { "DUNS3_2", "dom3_2_2.com", VAL_N, VAL_N, DOM_SRC, "Name3_2", 32000L }, //
            // try to clean up domain but actually no action taken since there
            // is no domain for duns-only entry)
            { "DUNS3_3", null, VAL_N, VAL_N, null, "Name3_3", 33000L }, //

            /*
             * Case 04: input duns in AMSeed only has one row and has domain
             * populated
             */
            // Add more rows
            { "DUNS4", "dom4.com", VAL_Y, VAL_Y, "DnB", "Name4", 40000L }, //
            { "DUNS4", "dom4_1.com", VAL_N, VAL_N, DOM_SRC, "Name4", 40000L }, //
            // Domain to add is same as existing domain, do nothing
            { "DUNS4_1", "dom4_1.com", VAL_Y, VAL_Y, "DnB", "Name4_1", 41000L }, //
            // Set domain to null
            { "DUNS4_2", null, VAL_N, VAL_N, null, "Name4_2", 42000L }, //
            // Domain to clean up does not exist, no action taken
            { "DUNS4_3", "dom4_3.com", VAL_Y, VAL_Y, "DnB", "Name4_3", 43000L }, //
            // Update domain - clean up existing domain by updating to
            // domain to add, also add more rows
            { "DUNS4_4", "dom4_4_1.com", VAL_N, VAL_N, DOM_SRC, "Name4_4", 44000L }, //
            { "DUNS4_4", "dom4_4_2.com", VAL_N, VAL_N, DOM_SRC, "Name4_4", 44000L }, //

            /*
             * Case 05: input duns in AMSeed has multiple rows and all of them
             * have domain populated
             */
            // Add more rows
            { "DUNS5_1", "dom5_1_1.com", VAL_Y, VAL_Y, "DnB", "Name5_1", 51000L }, //
            { "DUNS5_1", "dom5_1_2.com", VAL_N, VAL_Y, "DnB", "Name5_1", 51000L }, //
            { "DUNS5_1", "dom5_1_3.com", VAL_N, VAL_N, DOM_SRC, "Name5_1", 51000L }, //
            { "DUNS5_1", "dom5_1_4.com", VAL_N, VAL_N, DOM_SRC, "Name5_1", 51000L }, //
            // Domain to add is same as existing domain, no action taken
            { "DUNS5_2", "dom5_2_1.com", VAL_Y, VAL_Y, "DnB", "Name5_2", 52000L }, //
            { "DUNS5_2", "dom5_2_2.com", VAL_N, VAL_Y, "DnB", "Name5_2", 52000L }, //
            // Remove some rows
            { "DUNS5_3", "dom5_3_3.com", VAL_N, VAL_Y, "DnB", "Name5_3", 53000L }, //
            // Duns has 2 domains, set one of domain to null, remove the other
            // row
            { "DUNS5_4", null, VAL_N, VAL_N, null, "Name5_4", 54000L }, //
            // Domain to clean up does not exist
            { "DUNS5_5", "dom5_5_1.com", VAL_Y, VAL_Y, "DnB", "Name5_5", 55000L }, //
            { "DUNS5_5", "dom5_5_2.com", VAL_N, VAL_Y, "DnB", "Name5_5", 55000L }, //
            // Update domain - clean up existing domain by updating to domain to
            // add, also remove some rows
            { "DUNS5_6", "dom5_6_3.com", VAL_N, VAL_Y, "DnB", "Name5_6", 56000L }, //
            { "DUNS5_6", "dom5_6_4.com", VAL_N, VAL_N, DOM_SRC, "Name5_6", 56000L }, //

            /*
             * Case 06: input duns in AMSeed has multiple rows, one of them is
             * duns-only entry and others all have domain populated
             */
            // Remove all the rows with domain populated, leave duns-only
            // entry there
            { "DUNS6_1", null, VAL_N, VAL_Y, "DnB", "Name6_1", 61000L }, //
            // Populate duns-only entry with domain to add, also add some
            // rows and remove some rows
            { "DUNS6_2", "dom6_2_3.com", VAL_N, VAL_N, DOM_SRC, "Name6_2", 62000L }, //
            { "DUNS6_2", "dom6_2_4.com", VAL_N, VAL_N, DOM_SRC, "Name6_2", 62000L }, //
    };

    private void prepareDomainPatchBook() {
        // Only put columns which are used or might be impacted in dataflow
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(PatchBook.COLUMN_DUNS, String.class));
        schema.add(Pair.of(PatchBook.COLUMN_PATCH_ITEMS, String.class));
        schema.add(Pair.of(PatchBook.COLUMN_CLEANUP, Boolean.class));
        uploadBaseSourceData(domainPatch.getSourceName(), baseSourceVersion, schema, domainPatchBookData);
    }

    private void prepareAMS() {
        // Only put columns which are used or might be impacted in dataflow
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(DataCloudConstants.AMS_ATTR_DUNS, String.class));
        schema.add(Pair.of(DataCloudConstants.AMS_ATTR_DOMAIN, String.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_IS_PRIMARY_DOMAIN, String.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_IS_PRIMARY_LOCATION, String.class));
        schema.add(Pair.of(DataCloudConstants.AMS_ATTR_DOMAIN_SOURCE, String.class));
        // To test whether columns not used in dataflow could be retained
        // properly
        schema.add(Pair.of(DataCloudConstants.AMS_ATTR_NAME, String.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_SALES_VOL_US, Long.class));
        uploadBaseSourceData(ams.getSourceName(), baseSourceVersion, schema, amsData);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        Map<String, Object[]> expectedMap = new HashMap<>();
        for (Object[] data : expectedData) {
            String id = buildId(data[0], data[1]);
            expectedMap.put(id, data);
        }
        int count = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            String id = buildId(record.get(DataCloudConstants.AMS_ATTR_DUNS),
                    record.get(DataCloudConstants.AMS_ATTR_DOMAIN));
            Object[] expected = expectedMap.get(id);
            Assert.assertNotNull(expected);
            Assert.assertTrue(isObjEquals(record.get(DataCloudConstants.AMS_ATTR_DUNS), expected[0]));
            Assert.assertTrue(isObjEquals(record.get(DataCloudConstants.AMS_ATTR_DOMAIN), expected[1]));
            Assert.assertTrue(isObjEquals(record.get(DataCloudConstants.ATTR_IS_PRIMARY_DOMAIN), expected[2]));
            Assert.assertTrue(isObjEquals(record.get(DataCloudConstants.ATTR_IS_PRIMARY_LOCATION), expected[3]));
            Assert.assertTrue(isObjEquals(record.get(DataCloudConstants.AMS_ATTR_DOMAIN_SOURCE), expected[4]));
            Assert.assertTrue(isObjEquals(record.get(DataCloudConstants.AMS_ATTR_NAME), expected[5]));
            Assert.assertTrue(isObjEquals(record.get(DataCloudConstants.ATTR_SALES_VOL_US), expected[6]));
            count++;
        }
        Assert.assertEquals(count, expectedData.length);
    }

    // AMSeed is unique for domain + duns
    private String buildId(Object duns, Object domain) {
        return String.valueOf(duns) + String.valueOf(domain);
    }
}
