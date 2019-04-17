package com.latticeengines.datacloud.etl.transformation.service.impl.dunsredirect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.dunsredirect.DunsGuideBookRebuild;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.match.DunsGuideBook;
import com.latticeengines.domain.exposed.datacloud.match.DunsGuideBookConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.DunsRedirectBookConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class DunsGuideBookRebuildTestNG extends PipelineTransformationTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DunsGuideBookRebuildTestNG.class);

    private GeneralSource ams = new GeneralSource("AccountMasterSeed");
    private GeneralSource dunsRedirectFromDomDunsMap = new GeneralSource("DunsRedirectBook_DomDunsMap");
    private GeneralSource dunsRedirectFromHQ = new GeneralSource("DunsRedirectBook_HQ");
    private GeneralSource dunsRedirectFromMS = new GeneralSource("DunsRedirectBook_MS");
    private GeneralSource dunsGuideBook = new GeneralSource("DunsGuideBook");
    private GeneralSource source = dunsGuideBook;

    private static final String NAME_KEY = "Name";
    private static final String COUNTRY_KEY = "Country,Name";
    private static final String CITY_KEY = "City,Country,Name";
    private static final String STATE_KEY = "Country,Name,State";

    private static final String BOOKSRC_DOMDUNS = "DomainDunsMap";
    private static final String BOOKSRC_HQ = "DunsTree";
    private static final String BOOKSRC_MS = "ManualSeed";

    private static final String DUNS = DunsGuideBook.SRC_DUNS_KEY;
    private static final String ITEMS = DunsGuideBook.ITEMS_KEY;

    // To fake data
    private static final String DU_SUFFIX = "_DU";
    private static final String GU_SUFFIX = "_GU";

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
    protected String getTargetSourceName() {
        return source.getSourceName();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("BuildDunsRedirectBookFromDomDunsMap");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step0 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>();
        baseSources.add(ams.getSourceName());
        baseSources.add(dunsRedirectFromDomDunsMap.getSourceName());
        baseSources.add(dunsRedirectFromHQ.getSourceName());
        baseSources.add(dunsRedirectFromMS.getSourceName());
        step0.setBaseSources(baseSources);
        step0.setTransformer(DunsGuideBookRebuild.TRANSFORMER_NAME);
        step0.setTargetSource(source.getSourceName());
        step0.setConfiguration(getDunsGuideBookConfig());

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step0);

        // -----------
        configuration.setSteps(steps);
        return configuration;
    }

    @SuppressWarnings("serial")
    private String getDunsGuideBookConfig() {
        DunsGuideBookConfig config = new DunsGuideBookConfig();
        Map<String, Integer> bookPriority = new HashMap<String, Integer>() {
            {
                put(BOOKSRC_MS, 1);
                put(BOOKSRC_DOMDUNS, 2);
                put(BOOKSRC_HQ, 3);
            }
        };
        config.setBookPriority(bookPriority);
        return JsonUtils.serialize(config);
    }

    private void prepareData() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(DataCloudConstants.ATTR_LDC_DUNS, String.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_DU_DUNS, String.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_GU_DUNS, String.class));
        Object[][] amsData = new Object[][] {
                // Source Duns
                { "C1D001" }, { "C1D002" }, { "C1D003" }, { "C2D001" }, //
                { "C3D001" }, { "C4D001" }, { "C5D001" }, { "C6D001" }, //
                // Target Duns
                { "C1T001" }, { "C1T002" }, { "C1T003" }, { "C1T004" }, //
                { "C1T005" }, { "C1T006" }, { "C1T007" }, { "C1T008" }, //
                { "C2T001" }, { "C2T002" }, { "C2T003" }, //
                { "C3T001" }, { "C3T002" }, { "C3T003" }, //
                { "C4T001" }, { "C4T002" }, //
                { "C5T001" }, { "C5T002" }, { "C5T003" }, //
                { "C6T001" }, { "C6T002" }, //
                { "C7T001" }, //
                // Duns not present in DunsRedirectBook
                { "OtherDuns" }, //
        };
        Object[][] amsDataFinal = new Object[amsData.length][3];
        IntStream.range(0, amsData.length).forEach(i -> {
            System.arraycopy(amsData[i], 0, amsDataFinal[i], 0, amsData[i].length);
            amsDataFinal[i][1] = (String) amsDataFinal[i][0] + DU_SUFFIX;
            amsDataFinal[i][2] = (String) amsDataFinal[i][0] + GU_SUFFIX;
        });
        uploadBaseSourceData(ams.getSourceName(), baseSourceVersion, schema, amsDataFinal);

        schema = new ArrayList<>();
        schema.add(Pair.of(DunsRedirectBookConfig.DUNS, String.class));
        schema.add(Pair.of(DunsRedirectBookConfig.TARGET_DUNS, String.class));
        schema.add(Pair.of(DunsRedirectBookConfig.KEY_PARTITION, String.class));
        schema.add(Pair.of(DunsRedirectBookConfig.BOOK_SOURCE, String.class));

        Object[][] bookFromDomDunsMap = new Object[][] { //
                // Case 1: Duns not shared by other book
                { "C1D001", "C1T001", NAME_KEY, BOOKSRC_DOMDUNS }, //
                { "C1D001", "C1T002", COUNTRY_KEY, BOOKSRC_DOMDUNS }, //
                { "C1D001", "C1T003", STATE_KEY, BOOKSRC_DOMDUNS }, //

                // Case 2: Duns shared between all books without KeyPartition
                // conflict
                { "C2D001", "C2T001", COUNTRY_KEY, BOOKSRC_DOMDUNS }, //

                // Case 3: Duns shared between DomDunsBook & MSBook with
                // KeyPartition conflict
                { "C3D001", "C3T001", STATE_KEY, BOOKSRC_DOMDUNS }, // Has conflict
                { "C3D001", "C3T002", COUNTRY_KEY, BOOKSRC_DOMDUNS }, // No conflict

                // Case 4: Duns shared between DomDunsBook & HQBook with
                // KeyPartition conflict
                { "C4D001", "C4T001", NAME_KEY, BOOKSRC_DOMDUNS }, // Has conflict
                { "C4D001", "C4T002", STATE_KEY, BOOKSRC_DOMDUNS }, // No conflict

                // Case 6: Duns shared between all books with KeyPartition
                // conflict
                { "C6D001", "C6T001", NAME_KEY, BOOKSRC_DOMDUNS },

                // Case 7: Duns does not exist in AMSeed
                { "C7D001", "C7T001", NAME_KEY, BOOKSRC_DOMDUNS },
        };

        Object[][] bookFromMS = new Object[][] { //
                // Case 1: Duns not shared by other book
                { "C1D002", "C1T004", NAME_KEY, BOOKSRC_MS }, //
                { "C1D002", "C1T005", COUNTRY_KEY, BOOKSRC_MS }, //
                { "C1D002", "C1T006", STATE_KEY, BOOKSRC_MS }, //
                { "C1D002", "C1T007", CITY_KEY, BOOKSRC_MS }, //

                // Case 2: Duns shared between all books without KeyPartition
                // conflict
                { "C2D001", "C2T002", STATE_KEY, BOOKSRC_MS }, //

                // Case 3: Duns shared between DomDunsBook & MSBook with
                // KeyPartition conflict
                { "C3D001", "C3T003", STATE_KEY, BOOKSRC_MS }, // Has conflict

                // Case 5: Duns shared between MSBook & HQBook with
                // KeyPartition conflict
                { "C5D001", "C5T001", NAME_KEY, BOOKSRC_MS }, // Has conflict
                { "C5D001", "C5T002", CITY_KEY, BOOKSRC_MS }, // No conflict

                // Case 6: Duns shared between all books with KeyPartition
                // conflict
                { "C6D001", "C6T002", NAME_KEY, BOOKSRC_MS },

                // Case 8: TargetDuns does not exist in AMSeed
                { "C8D001", "C8T001", NAME_KEY, BOOKSRC_MS },
        };

        Object[][] bookFromHQ = new Object[][] { //
                // Case 1: Duns not shared by other book
                { "C1D003", "C1T008", NAME_KEY, BOOKSRC_HQ }, //

                // Case 2: Duns shared between all books without KeyPartition
                // conflict
                { "C2D001", "C2T003", NAME_KEY, BOOKSRC_HQ }, //

                // Case 4: Duns shared between DomDunsBook & HQBook with
                // KeyPartition conflict
                { "C4D001", "C4T003", NAME_KEY, BOOKSRC_HQ }, // Has conflict

                // Case 5: Duns shared between MSBook & HQBook with
                // KeyPartition conflict
                { "C5D001", "C5T003", NAME_KEY, BOOKSRC_HQ }, // Has conflict

                // Case 6: Duns shared between all books with KeyPartition
                // conflict
                { "C6D001", "C6T001", NAME_KEY, BOOKSRC_HQ },

                // Case 9: Both Duns and TargetDuns do not exist in AMSeed
                { "C9D001", "C9T001", NAME_KEY, BOOKSRC_HQ },
        };
        uploadBaseSourceData(dunsRedirectFromDomDunsMap.getSourceName(), baseSourceVersion, schema, bookFromDomDunsMap);
        uploadBaseSourceData(dunsRedirectFromMS.getSourceName(), baseSourceVersion, schema, bookFromMS);

        // Test merging DunsRedirectBook with schema out of order
        List<Pair<String, Class<?>>> schemaOutOfOrder = new ArrayList<>();
        for (int i = schema.size() - 1; i >= 0; i--) {
            schemaOutOfOrder.add(schema.get(i));
        }
        Object[][] bookFromHQOutOfOrder = new Object[bookFromHQ.length][bookFromHQ[0].length];
        for (int i = 0; i < bookFromHQ.length; i++)
            for (int j = bookFromHQ[0].length - 1; j >= 0; j--) {
                bookFromHQOutOfOrder[i][bookFromHQ[0].length - 1 - j] = bookFromHQ[i][j];
            }
        uploadBaseSourceData(dunsRedirectFromHQ.getSourceName(), baseSourceVersion, schemaOutOfOrder,
                bookFromHQOutOfOrder);
    }

    private Map<String, List<DunsGuideBook.Item>> constructExpectedDunsGuideBook() {
        Map<String, List<DunsGuideBook.Item>> map = new HashMap<>();

        String[][] depivoted = new String[][] {
                // Case 1: Duns not shared by other book
                { "C1D001", "C1T001", NAME_KEY, BOOKSRC_DOMDUNS }, //
                { "C1D001", "C1T002", COUNTRY_KEY, BOOKSRC_DOMDUNS }, //
                { "C1D001", "C1T003", STATE_KEY, BOOKSRC_DOMDUNS }, //
                { "C1D002", "C1T004", NAME_KEY, BOOKSRC_MS }, //
                { "C1D002", "C1T005", COUNTRY_KEY, BOOKSRC_MS }, //
                { "C1D002", "C1T006", STATE_KEY, BOOKSRC_MS }, //
                { "C1D002", "C1T007", CITY_KEY, BOOKSRC_MS }, //
                { "C1D003", "C1T008", NAME_KEY, BOOKSRC_HQ }, //

                // Case 2: Duns shared between all books without KeyPartition
                // conflict
                { "C2D001", "C2T001", COUNTRY_KEY, BOOKSRC_DOMDUNS }, //
                { "C2D001", "C2T002", STATE_KEY, BOOKSRC_MS }, //
                { "C2D001", "C2T003", NAME_KEY, BOOKSRC_HQ }, //

                // Case 3: Duns shared between DomDunsBook & MSBook with
                // KeyPartition conflict
                { "C3D001", "C3T003", STATE_KEY, BOOKSRC_MS }, // Choose from MSBook
                { "C3D001", "C3T002", COUNTRY_KEY, BOOKSRC_DOMDUNS }, // No conflict

                // Case 4: Duns shared between DomDunsBook & HQBook with
                // KeyPartition conflict
                { "C4D001", "C4T001", NAME_KEY, BOOKSRC_DOMDUNS }, // Choose from DomDunsBook
                { "C4D001", "C4T002", STATE_KEY, BOOKSRC_DOMDUNS }, // No conflict

                // Case 5: Duns shared between MSBook & HQBook with
                // KeyPartition conflict
                { "C5D001", "C5T001", NAME_KEY, BOOKSRC_MS }, // Choose from MSBook
                { "C5D001", "C5T002", CITY_KEY, BOOKSRC_MS }, // No conflict

                // Case 6: Duns shared between all books with KeyPartition
                // conflict
                { "C6D001", "C6T002", NAME_KEY, BOOKSRC_MS }, // Choose from MSBook

                // Case 7: Duns does not exist in AMSeed, should still retain in
                // AMSeed
                { "C7D001", "C7T001", NAME_KEY, BOOKSRC_DOMDUNS },

                // Case 8 & 9 are not present in result because target duns does
                // not exist
        };

        for (String[] item : depivoted) {
            String id = buildIdentifier(item[0]);
            if (!map.containsKey(id)) {
                map.put(id, new ArrayList<>());
            }
            map.get(id)
                    .add(new DunsGuideBook.Item(item[1], item[1] + DU_SUFFIX, item[1] + GU_SUFFIX, item[2], item[3]));
        }

        String[] dunsWithoutGuideBook = { //
                "C1T001", "C1T002", "C1T003", "C1T004", //
                "C1T005", "C1T006", "C1T007", "C1T008", //
                "C2T001", "C2T002", "C2T003", //
                "C3T001", "C3T002", "C3T003", //
                "C4T001", "C4T002", //
                "C5T001", "C5T002", "C5T003", //
                "C6T001", "C6T002", //
                "C7T001", //
                "OtherDuns", //
        };
        for (String duns : dunsWithoutGuideBook) {
            map.put(buildIdentifier(duns), null);
        }
        return map;
    }

    private String buildIdentifier(String duns) {
        String duDuns = duns + DU_SUFFIX;
        String guDuns = duns + GU_SUFFIX;
        return duns + duDuns + guDuns;
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        Map<String, List<DunsGuideBook.Item>> expected = constructExpectedDunsGuideBook();
        Set<String> visitedIds = new HashSet<>();
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            String duns = record.get(DUNS).toString();
            String id = buildIdentifier(duns);
            Assert.assertTrue(expected.containsKey(id));
            List<DunsGuideBook.Item> books = null;
            if (record.get(ITEMS) != null) {
                String items = record.get(ITEMS).toString();
                List<?> list = JsonUtils.deserialize(items, List.class);
                books = JsonUtils.convertList(list, DunsGuideBook.Item.class);
            }
            verifyDunsGuideBooks(books, expected.get(id));
            visitedIds.add(id);
        }
        Assert.assertEquals(visitedIds.size(), expected.size());
    }

    private void verifyDunsGuideBooks(List<DunsGuideBook.Item> actual, List<DunsGuideBook.Item> expected) {
        if (expected == null) {
            Assert.assertNull(actual);
            return;
        }
        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.size(), expected.size());

        Comparator<DunsGuideBook.Item> comparator = Comparator.comparing(DunsGuideBook.Item::getKeyPartition) //
                .thenComparing(DunsGuideBook.Item::getDuns) //
                .thenComparing(DunsGuideBook.Item::getDuDuns) //
                .thenComparing(DunsGuideBook.Item::getGuDuns) //
                .thenComparing(DunsGuideBook.Item::getBookSource);
        Collections.sort(actual, comparator);
        Collections.sort(expected, comparator);
        Assert.assertEquals(JsonUtils.serialize(actual), JsonUtils.serialize(expected));
    }


}
