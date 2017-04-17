package com.latticeengines.serviceflows.dataflow;

import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_DEDUPE_ID;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_LID;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_LOC_CHECKSUM;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_POPULATED_ATTRS;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_PREMATCH_DOMAIN;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.match.ParseMatchResultParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-core-context.xml" })
public class ParseMatchResultDedupeIdTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

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
                Pair.of("ID", Integer.class), //
                Pair.of(INT_LDC_LID, String.class), //
                Pair.of(INT_LDC_PREMATCH_DOMAIN, String.class), //
                Pair.of(INT_LDC_LOC_CHECKSUM, String.class), //
                Pair.of(INT_LDC_POPULATED_ATTRS, Integer.class), //
                Pair.of("ExpectedDedupeId", String.class) //
        );
        uploadDataToSharedAvroInput(getData(), fields);

        ParseMatchResultParameters parameters = new ParseMatchResultParameters();
        parameters.sourceTableName = AVRO_INPUT;
        parameters.sourceColumns = Collections.emptyList();

        return parameters;
    }

    private Object[][] getData() {
        return new Object[][] { //
                { 1, "1", "dom1.com", "a", 10, "1" }, //
                { 2, "2", "dom2.com", "b", 10, "2" }, //
                { 3, "3", "dom3.com", "c", 10, "3" }, //
                { 4, "4", "dom3.com", "d", 5, "4" }, //
                { 5, "5", null, "c", 10, "5" }, //
                { 6, "6", "dom4.com", "b", 5, "6" }, //

                // domain same as a matched row
                { 7, null, "dom1.com", "b", 0, "1" }, //
                { 8, null, "dom2.com", "n", 0, "2" }, //
                { 9, null, "dom3.com", "n", 0, "3" }, //

                // location only
                { 10, null, null, "a", 0, "1" }, //  we have only one row matched for 'a'
                { 11, null, null, "b", 0, "2" }, // two rows with 'b', pick the one with high pop
                { 12, null, null, "z", 0, "z" }, // unmatched location

                // both valid, but domain not matched
                { 13, null, "dom99.com", "a", 0, "dom99.coma" }, // valid domain, not matched, use domain + checksum
                { 14, null, "dom98.com", "b", 0, "dom98.comb" }, // valid domain, not matched, use domain + checksum
        };
    }

    private void verifyResult() {
        int numRows = 0;
        List<GenericRecord> records = readOutput();
        for (GenericRecord record : records) {
            System.out.println(record);
            Assert.assertEquals(record.get(INT_LDC_DEDUPE_ID), record.get("ExpectedDedupeId"));
            numRows++;
        }
        Assert.assertEquals(numRows, getData().length);
    }

}
