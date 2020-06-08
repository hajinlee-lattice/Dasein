package com.latticeengines.spark.exposed.job.dcp;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.dcp.InputPresenceConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class InputPresenceJobTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(InputPresenceJobTestNG.class);

    private static final List<Pair<String, Class<?>>> INPUT_FIELDS = Arrays.asList(
            Pair.of(InterfaceName.Id.name(), String.class),
            Pair.of(InterfaceName.CompanyName.name(), String.class), //
            Pair.of(InterfaceName.City.name(), String.class), //
            Pair.of(InterfaceName.State.name(), String.class), //
            Pair.of(InterfaceName.Country.name(), String.class), //
            Pair.of(InterfaceName.PostalCode.name(), String.class), //
            Pair.of(InterfaceName.PhoneNumber.name(), String.class), //
            Pair.of(InterfaceName.DUNS.name(), String.class) //
    );

    @Test(groups = "functional")
    private void test() {
        uploadData();
        InputPresenceConfig config = new InputPresenceConfig();
        config.setInputNames(new HashSet<>(
                Arrays.asList(InterfaceName.Id.name(),
                        InterfaceName.CompanyName.name(),
                        InterfaceName.City.name(),
                        InterfaceName.State.name(),
                        InterfaceName.Country.name(),
                        InterfaceName.PostalCode.name(),
                        InterfaceName.PhoneNumber.name(),
                        InterfaceName.DUNS.name())));

        SparkJobResult result = runSparkJob(InputPresenceJob.class, config);
        verifyResult(result);
    }


    private void uploadData() {
        Object[][] data = new Object[][] {
                {"12", "Abu Dhabi Islamic Bank", "Abu Dhabi", "UC", "United Arab Emirates", "", "234-125", "420959126"},
                {"7", "Abbott Laboratories", "Abbott Park", "Illinois", "United States", "60064-6400", null, "  "},
                {"8", "AbbVie", "North Chicago", "Illinois", "United States", null, null, null},
                {"9", "Abertis", "Barcelona", null, "Spain", null, "8040", null},
                {"10", "Google", "San Mateo", "UC", "United States", null, null, "480959126"},
                {"11", "AliBaba", "Hangzhou", "Zhejiang", "China", "1000891", "955-566", null},
        };
        uploadHdfsDataUnit(data, INPUT_FIELDS);
    }


    @Override
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Collections.EMPTY_LIST;
    }

    @Override
    protected void verifyOutput(String output) {
        Map<String, Long> map = JsonUtils.convertMap(JsonUtils.deserialize(output, Map.class), String.class,
                Long.class);
        Assert.assertEquals(map.get(InterfaceName.Id.name()), Long.valueOf(6));
        Assert.assertEquals(map.get(InterfaceName.CompanyName.name()), Long.valueOf(6));
        Assert.assertEquals(map.get(InterfaceName.City.name()), Long.valueOf(6));
        Assert.assertEquals(map.get(InterfaceName.State.name()), Long.valueOf(5));
        Assert.assertEquals(map.get(InterfaceName.Country.name()), Long.valueOf(6));
        Assert.assertEquals(map.get(InterfaceName.PostalCode.name()), Long.valueOf(2));
        Assert.assertEquals(map.get(InterfaceName.PhoneNumber.name()), Long.valueOf(3));
        Assert.assertEquals(map.get(InterfaceName.DUNS.name()), Long.valueOf(2));
    }
}
