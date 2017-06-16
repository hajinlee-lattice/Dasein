package com.latticeengines.datacloud.dataflow.transformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.domain.exposed.datacloud.dataflow.SorterParameters;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

public class SortTestNG extends DataCloudDataFlowFunctionalTestNGBase {

    private static final int NUM_ROWS = 24;
    private static final int PARTITIONS = 8;

    @Override
    protected String getFlowBeanName() {
        return Sort.BEAN_NAME;
    }

    @Test(groups = "functional")
    public void test() throws Exception {
        SorterParameters parameters = prepareInput();
        executeDataFlow(parameters);
        verifyResult();
    }

    private SorterParameters prepareInput() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("RowID", Long.class), //
                Pair.of("Field", String.class)
        );
        List<Long> ids = new ArrayList<>();
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < NUM_ROWS; i++) {
            ids.add((long) (i + 1));
            if (random.nextInt(100) > 50) {
                ids.add(null);
            }
        }
        Collections.shuffle(ids);
        Object[][] data = new Object[NUM_ROWS][2];
        for (int i = 0; i < NUM_ROWS; i++) {
            data[i][0] = ids.get(i);
            data[i][1] = String.format("%d-%s", ids.get(i), UUID.randomUUID().toString().substring(0, 8));
        }
        uploadDataToSharedAvroInput(data, fields);

        SorterParameters parameters = new SorterParameters();
        parameters.setBaseTables(Collections.singletonList(AVRO_INPUT));
        parameters.setSortingField("RowID");
        parameters.setPartitions(PARTITIONS);
        parameters.setPartitionField("_Partition_");
        return parameters;
    }

    @Override
    protected DataFlowContext updateDataFlowContext(DataFlowContext ctx) {
        ctx.setProperty(DataFlowProperty.PARTITIONS, PARTITIONS);
        return ctx;
    }

    private void verifyResult() throws IOException {
        String glob = getTargetDirectory() + "/*.avro";
        List<String> files = HdfsUtils.getFilesByGlob(yarnConfiguration, glob);
        for (String file: files) {
            System.out.println(file);
            List<GenericRecord> records = AvroUtils.getDataFromGlob(yarnConfiguration, file);
            records.forEach(System.out::println);
        }
    }
}
