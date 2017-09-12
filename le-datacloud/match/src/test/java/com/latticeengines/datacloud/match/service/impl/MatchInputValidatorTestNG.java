package com.latticeengines.datacloud.match.service.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;

@Component
public class MatchInputValidatorTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final String avroDir = "/tmp/MatchInputValidatorTestNG";
    private static final String fileName = "BulkMatchInput.avro";

    @Test(groups = "functional")
    public void testBulkInputWithAvroBuffer() {
        cleanupAvroDir(avroDir);

        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant("PD_Test"));
        AvroInputBuffer inputBuffer = new AvroInputBuffer();
        inputBuffer.setAvroDir(avroDir);
        matchInput.setInputBuffer(inputBuffer);

        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.Domain, Collections.singletonList("Website"));
        matchInput.setKeyMap(keyMap);

        boolean failed = false;
        try {
            MatchInputValidator.validateBulkInput(matchInput, yarnConfiguration);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed, "Should failed on missing field.");

        matchInput.setKeyMap(new HashMap<MatchKey, List<String>>());

        failed = false;
        try {
            MatchInputValidator.validateBulkInput(matchInput, yarnConfiguration);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed, "Should failed on missing avro.");

        uploadDataCsv(avroDir, fileName);
        failed = false;
        try {
            MatchInputValidator.validateBulkInput(matchInput, yarnConfiguration);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed, "Should failed on missing selection.");

        matchInput.setPredefinedSelection(Predefined.DerivedColumns);
        try {
            MatchInputValidator.validateBulkInput(matchInput, yarnConfiguration);
        } catch (Exception e) {
            Assert.fail("Validation failed", e);
        }

        cleanupAvroDir(avroDir);
    }

}
