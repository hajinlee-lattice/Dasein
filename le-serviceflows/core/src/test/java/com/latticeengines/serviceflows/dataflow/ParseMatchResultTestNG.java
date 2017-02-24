package com.latticeengines.serviceflows.dataflow;

import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_LOC_CHECKSUM;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.LID_FIELD;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.SOURCE_PREFIX;

import java.util.Arrays;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.match.ParseMatchResultParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-core-context.xml" })
public class ParseMatchResultTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        ParseMatchResultParameters parameters = prepareInput();
        executeDataFlow(parameters);
        verifyResult();
    }

    @Override
    protected String getFlowBeanName() {
        return "parseMatchResult";
    }

    private ParseMatchResultParameters prepareInput() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(SOURCE_PREFIX + "Domain", String.class), //
                Pair.of("Name", String.class), //
                Pair.of("City", String.class), //
                Pair.of("State", String.class), //
                Pair.of("Country", String.class), //
                Pair.of(LID_FIELD, Long.class), //
                Pair.of("Domain", String.class), //
                Pair.of(INT_LDC_LOC_CHECKSUM, String.class) //
        );
        Object[][] data = new Object[][] { //
                { "dom1.com", "Name1", "City1", "State1", "Country1", 1L, "d.com", "a" }, //
                { "dom2.com", "Name2", "City2", "State2", "Country2", 2L, "d.com", "b" }, //
                { "dom3.com", "Name3", "City3", "State3", "Country3", 3L, "d.com", "c" }
        };
        uploadDataToSharedAvroInput(data, fields);

        ParseMatchResultParameters parameters = new ParseMatchResultParameters();
        parameters.sourceTableName = AVRO_INPUT;
        parameters.sourceColumns = Arrays.asList( //
                "Domain", //
                "Name", //
                "City", //
                "State", //
                "Country" //
        );
        parameters.excludeDataCloudAttrs = true;

        return parameters;
    }

    private void verifyResult() {
        List<GenericRecord> records = readOutput();
        int numRows = 0;
        for (GenericRecord record : records) {
            System.out.println(record);
            Assert.assertNull(record.get(INT_LDC_LOC_CHECKSUM),
                    "Internal attribute " + INT_LDC_LOC_CHECKSUM + " should be removed.");
            Assert.assertFalse("d.com".equals(record.get("Domain")));
            Assert.assertNull(record.get(LID_FIELD));
            numRows++;
        }
        Assert.assertEquals(numRows, 3);
    }

}
