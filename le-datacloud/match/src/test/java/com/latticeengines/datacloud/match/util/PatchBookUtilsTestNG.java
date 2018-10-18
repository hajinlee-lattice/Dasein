package com.latticeengines.datacloud.match.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook.Type;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchBookValidationError;

public class PatchBookUtilsTestNG {
    private static final String DUPLICATE_MATCH_KEY_ERROR = "Duplicate match key combination found : ";

    @Test(groups = "unit")
    public void testDuplicateMatchKey() {
        Object[][] inputData = provideBatchData();
        List<PatchBook> patchBookList = new ArrayList<>();
        for (int row = 0; row < inputData.length; row++) {
            PatchBook patchBook = new PatchBook();
            patchBook.setType((Type) inputData[row][0]);
            patchBook.setPid((Long) inputData[row][1]);
            patchBook.setDomain((String) inputData[row][2]);
            patchBook.setDuns((String) (inputData[row][3]));
            patchBookList.add(patchBook);
        }
        List<PatchBookValidationError> errorList = PatchBookUtils.validateDuplicateMatchKey(patchBookList);
        // expected
        Map<String, List<Long>> expectedMap = expectedDataSet();
        for (PatchBookValidationError e : errorList) {
            List<Long> pidList = expectedMap.get(e.getMessage());
            Assert.assertNotNull(pidList);
            Collections.sort(e.getPatchBookIds());
            Assert.assertEquals(e.getPatchBookIds(), pidList);
        }
    }

    private Map<String, List<Long>> expectedDataSet() {
        return ImmutableMap.of(DUPLICATE_MATCH_KEY_ERROR + "DUNS=124124124,Domain=abc.com", Arrays.asList(1L, 2L, 5L),
                DUPLICATE_MATCH_KEY_ERROR + "DUNS=328522482,Domain=def.com", Arrays.asList(3L, 6L),
                DUPLICATE_MATCH_KEY_ERROR + "Domain=lmn.com", Arrays.asList(9L, 10L),
                DUPLICATE_MATCH_KEY_ERROR + "DUNS=429489284", Arrays.asList(11L, 12L));
    }

    private Object[][] provideBatchData() {
        return new Object[][] { // Testing Attribute Patch Validate API Match Key
                // Duplicate domain+duns
                { PatchBook.Type.Attribute, 1L, "abc.com", "124124124" },
                { PatchBook.Type.Attribute, 2L, "abc.com", "124124124" },
                { PatchBook.Type.Attribute, 5L, "abc.com", "124124124" },
                { PatchBook.Type.Attribute, 3L, "def.com", "328522482" },
                { PatchBook.Type.Attribute, 6L, "def.com", "328522482" },
                // distinct domain only
                { PatchBook.Type.Attribute, 4L, "abc.com", null },
                // distinct duns only
                { PatchBook.Type.Attribute, 7L, null, "124124124" },
                // distinct domain + duns
                { PatchBook.Type.Attribute, 8L, "ghi.com", "127947873" },
                // Duplicate domain only
                { PatchBook.Type.Attribute, 9L, "lmn.com", null },
                { PatchBook.Type.Attribute, 10L, "lmn.com", null },
                // Duplicate duns only
                { PatchBook.Type.Attribute, 11L, null, "429489284" },
                { PatchBook.Type.Attribute, 12L, null, "429489284" }, };
    }
}
