package com.latticeengines.leadprioritization.dataflow;

import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_DEDUPE_ID;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.LID_FIELD;
import static com.latticeengines.leadprioritization.dataflow.DedupEventTable.OPTIMAL_CREATION_TIME_DAYS_FROM_TODAY;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.dataflow.flows.DedupEventTableParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-leadprioritization-context.xml" })
public class DedupEventTableTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    private static final String LM = "LastModified";

    @Test(groups = "functional")
    public void test() {
        DedupEventTableParameters parameters = prepareInput();
        executeDataFlow(parameters);
        verifyResult();
    }

    @Override
    protected String getFlowBeanName() {
        return "dedupEventTable";
    }

    private DedupEventTableParameters prepareInput() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("ID", Integer.class), //
                Pair.of("Event", Boolean.class), //
                Pair.of(LID_FIELD, Long.class), //
                Pair.of(LM, Long.class), //
                Pair.of(INT_LDC_DEDUPE_ID, String.class),
                Pair.of("ExpectedToStay", Boolean.class) //
        );
        uploadDataToSharedAvroInput(getData(), fields);

        DedupEventTableParameters parameters = new DedupEventTableParameters();
        parameters.eventTable = AVRO_INPUT;
        return parameters;
    }

    private Object[][] getData() {
        return new Object[][] { //
                { 1, false, 1L,  dayShift(10), "1", true }, // has-lm has highest priority
                { 2, true, 2L,  null, "1", false }, //

                { 4, true, 4L,  dayShift(2), "2", true }, // then it is event
                { 5, false, 5L,  dayShift(0), "2", false }, //
                { 6, true, 6L,  dayShift(3), "2", false }, //

                { 7, true, 7L,  dayShift(5), "3", true }, // lastly it is lm dist
                { 8, true, 8L,  dayShift(10), "3", false }, //
        };
    }

    private Long dayShift(int dayshifts) {
        Long millsShift = TimeUnit.DAYS.toMillis(- OPTIMAL_CREATION_TIME_DAYS_FROM_TODAY + dayshifts);
        return System.currentTimeMillis() + millsShift;
    }

    private void verifyResult() {
        int numRows = 0;
        List<GenericRecord> records = readOutput();
        for (GenericRecord record : records) {
            System.out.println(record);
            Assert.assertTrue((Boolean) record.get("ExpectedToStay"));
            numRows++;
        }
        Assert.assertEquals(numRows, 3);
    }

    @Override
    protected String getLastModifiedColumnName(String tableName) {
        return LM;
    }
}
