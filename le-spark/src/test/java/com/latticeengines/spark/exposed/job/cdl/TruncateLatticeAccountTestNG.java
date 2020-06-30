package com.latticeengines.spark.exposed.job.cdl;

import static com.latticeengines.domain.exposed.spark.common.ChangeListConstants.ColumnId;
import static com.latticeengines.domain.exposed.spark.common.ChangeListConstants.RowId;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.TruncateLatticeAccountConfig;
import com.latticeengines.domain.exposed.spark.common.ChangeListConstants;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class TruncateLatticeAccountTestNG extends SparkJobFunctionalTestNGBase {

    private static final String TEST_CASE_1 = "base";
    private static final String TEST_CASE_2 = "removeAttr2";
    private static final String TEST_CASE_3 = "ignoreChangeList";
    private static final String TEST_CASE_4 = "emptyChangeList";

    private Function<HdfsDataUnit, Boolean> deleteVerifier;
    private Function<HdfsDataUnit, Boolean> changeListVerifier;

    @Test(groups = "functional")
    public void test() {
        uploadBaseData();
        TruncateLatticeAccountConfig config = new TruncateLatticeAccountConfig();
        config.setIgnoreAttrs(Arrays.asList( //
                InterfaceName.CDLCreatedTime.name(), //
                InterfaceName.CDLUpdatedTime.name(), //
                InterfaceName.AccountId.name(), //
                InterfaceName.LatticeAccountId.name() //
        ));
        config.setIgnoreRemoveAttrsChangeList(true);
        SparkJobResult result = runSparkJob(TruncateLatticeAccount.class, config);
        deleteVerifier = tgt -> verifyDeleted(tgt, TEST_CASE_4);
        changeListVerifier = tgt -> verifyChangeList(tgt, TEST_CASE_4);
        verifyResult(result);

        uploadDeletChangeList();
        config.setIgnoreRemoveAttrsChangeList(null);
        result = runSparkJob(TruncateLatticeAccount.class, config);
        deleteVerifier = tgt -> verifyDeleted(tgt, TEST_CASE_1);
        changeListVerifier = tgt -> verifyChangeList(tgt, TEST_CASE_1);
        verifyResult(result);

        config.setRemoveAttrs(Collections.singletonList("Attr2"));
        result = runSparkJob(TruncateLatticeAccount.class, config);
        deleteVerifier = tgt -> verifyDeleted(tgt, TEST_CASE_2);
        changeListVerifier = tgt -> verifyChangeList(tgt, TEST_CASE_2);
        verifyResult(result);

        config.setIgnoreRemoveAttrsChangeList(true);
        result = runSparkJob(TruncateLatticeAccount.class, config);
        deleteVerifier = tgt -> verifyDeleted(tgt, TEST_CASE_3);
        changeListVerifier = tgt -> verifyChangeList(tgt, TEST_CASE_3);
        verifyResult(result);
    }

    private void uploadBaseData() {
        List<Pair<String, Class<?>>> fields1 = Arrays.asList( //
                Pair.of(InterfaceName.AccountId.name(), String.class), //
                Pair.of(InterfaceName.LatticeAccountId.name(), String.class), //
                Pair.of("Attr1", String.class), //
                Pair.of("Attr2", String.class), //
                Pair.of(InterfaceName.CDLCreatedTime.name(), Long.class), //
                Pair.of(InterfaceName.CDLUpdatedTime.name(), Long.class));

        Object[][] upload1 = new Object[][] {
                { "Account1", "Lattice1", "1_1", "2_1", 1L, 1L },
                { "Account2", "Lattice2", "1_2", "2_2", 1L, 1L },
                { "Account3", "Lattice3", "1_3", "2_3", 1L, 1L },
                { "Account4", "Lattice4", "1_4", "2_4", 1L, 1L },
                { "Account5", "Lattice5", "1_5", "2_5", 1L, 1L },
                { "Account6", "Lattice6", "1_6", "2_6", 1L, 1L },
        };
        uploadHdfsDataUnit(upload1, fields1);
    }

    private void uploadDeletChangeList() {
        List<Pair<String, Class<?>>> fields2 = ChangeListConstants.schema();
        Object[][] upload2 = new Object[][] { //
                { "Account1", InterfaceName.LatticeAccountId.name(), "String", true, "Lattice1", null, null, null, null, null, null, null,
                        null, null, null, null },
                { "Account3", InterfaceName.LatticeAccountId.name(), "String", true, "Lattice3", null, null, null, null, null, null, null,
                        null, null, null, null },
        };
        uploadHdfsDataUnit(upload2, fields2);
    }

    @Override
    public List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Arrays.asList(deleteVerifier, changeListVerifier);
    }

    private Boolean verifyDeleted(HdfsDataUnit tgt, String testCase) {
        Set<String> keys;
        if (TEST_CASE_4.equals(testCase)) {
            keys = new HashSet<>(Arrays.asList("Account1", "Account2", "Account3", "Account4", "Account5", "Account6"));
        } else {
            keys = new HashSet<>(Arrays.asList("Account2", "Account4", "Account5", "Account6"));
        }
        Set<String> keysToBeFound = new HashSet<>(keys);
        Set<String> expectedKeys = new HashSet<>(keys);
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            // System.out.println(record);
            String key = record.get(InterfaceName.AccountId.name()).toString();
            Assert.assertTrue(expectedKeys.contains(key), "Not expecting account id " + key);
            keysToBeFound.remove(key);
            if (TEST_CASE_2.equals(testCase)) {
                Assert.assertNull(record.get("Attr2"));
            }
        });
        Assert.assertTrue(keysToBeFound.isEmpty(), "Not seen account ids " + keysToBeFound);
        return true;
    }

    private Boolean verifyChangeList(HdfsDataUnit tgt, String testCase) {
        Set<String> keys = new HashSet<>();
        if (TEST_CASE_1.equals(testCase)) {
            keys = new HashSet<>(Arrays.asList( //
                    "Account1-#", "Account1-Attr1", "Account1-Attr2", //
                    "Account3-#", "Account3-Attr1", "Account3-Attr2" //
            ));
        } else if (TEST_CASE_2.equals(testCase)) {
            keys = new HashSet<>(Arrays.asList( //
                    "Account1-#", "Account1-Attr1", //
                    "Account3-#", "Account3-Attr1", //
                    "#-Attr2"
            ));
        } else if (TEST_CASE_3.equals(testCase)) {
            keys = new HashSet<>(Arrays.asList( //
                    "Account1-#", "Account1-Attr1", //
                    "Account3-#", "Account3-Attr1" //
            ));
        } else if (TEST_CASE_4.equals(testCase)) {
            keys = new HashSet<>();
        } else {
            Assert.fail("Unknown test case " + testCase);
        }
        Set<String> keysToBeFound = new HashSet<>(keys);
        Set<String> expectedKeys = new HashSet<>(keys);
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            // System.out.println(record);
            Assert.assertEquals(record.getSchema().getFields().size(), 16, record.toString());
            String rowId = record.get(RowId) != null ? record.get(RowId).toString() : "#";
            String columnId = record.get(ColumnId) != null ? record.get(ColumnId).toString() : "#";
            String key = rowId + "-" + columnId;
            Assert.assertTrue(expectedKeys.contains(key), "Not expecting key " + key);
            keysToBeFound.remove(key);
        });
        Assert.assertTrue(keysToBeFound.isEmpty(), "Not seen keys " + keysToBeFound);
        return true;
    }

}
