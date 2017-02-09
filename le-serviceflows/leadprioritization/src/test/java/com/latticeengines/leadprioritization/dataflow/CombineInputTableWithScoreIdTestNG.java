package com.latticeengines.leadprioritization.dataflow;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.dataflow.flows.CombineInputTableWithScoreParameters;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-leadprioritization-context.xml" })
public class CombineInputTableWithScoreIdTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    private static final Double LIFT_1 = 3.4;
    private static final Double LIFT_2 = 2.4;
    private static final Double LIFT_3 = 1.2;
    private static final Double LIFT_4 = 0.4;

    private static final int NUM_LEADS_BUCKET_1 = 28588;
    private static final int NUM_LEADS_BUCKET_2 = 14534;
    private static final int NUM_LEADS_BUCKET_3 = 25206;
    private static final int NUM_LEADS_BUCKET_4 = 25565;

    private CombineInputTableWithScoreParameters getStandardParameters() {
        CombineInputTableWithScoreParameters params = new CombineInputTableWithScoreParameters("ScoreResult",
                "InputTable", generateDefaultBucketMetadata());
        return params;
    }

    private List<BucketMetadata> generateDefaultBucketMetadata() {
        List<BucketMetadata> bucketMetadataList = new ArrayList<BucketMetadata>();
        BucketMetadata BUCKET_METADATA_A = new BucketMetadata();
        BucketMetadata BUCKET_METADATA_B = new BucketMetadata();
        BucketMetadata BUCKET_METADATA_C = new BucketMetadata();
        BucketMetadata BUCKET_METADATA_D = new BucketMetadata();
        bucketMetadataList.add(BUCKET_METADATA_A);
        bucketMetadataList.add(BUCKET_METADATA_B);
        bucketMetadataList.add(BUCKET_METADATA_C);
        bucketMetadataList.add(BUCKET_METADATA_D);
        BUCKET_METADATA_A.setBucketName(BucketName.A);
        BUCKET_METADATA_A.setNumLeads(NUM_LEADS_BUCKET_1);
        BUCKET_METADATA_A.setLeftBoundScore(99);
        BUCKET_METADATA_A.setRightBoundScore(95);
        BUCKET_METADATA_A.setLift(LIFT_1);
        BUCKET_METADATA_B.setBucketName(BucketName.B);
        BUCKET_METADATA_B.setNumLeads(NUM_LEADS_BUCKET_2);
        BUCKET_METADATA_B.setLeftBoundScore(94);
        BUCKET_METADATA_B.setRightBoundScore(85);
        BUCKET_METADATA_B.setLift(LIFT_2);
        BUCKET_METADATA_C.setBucketName(BucketName.C);
        BUCKET_METADATA_C.setNumLeads(NUM_LEADS_BUCKET_3);
        BUCKET_METADATA_C.setLeftBoundScore(84);
        BUCKET_METADATA_C.setRightBoundScore(50);
        BUCKET_METADATA_C.setLift(LIFT_3);
        BUCKET_METADATA_D.setBucketName(BucketName.D);
        BUCKET_METADATA_D.setNumLeads(NUM_LEADS_BUCKET_4);
        BUCKET_METADATA_D.setLeftBoundScore(49);
        BUCKET_METADATA_D.setRightBoundScore(5);
        BUCKET_METADATA_D.setLift(LIFT_4);
        return bucketMetadataList;
    }

    @Override
    protected String getFlowBeanName() {
        return "combineInputTableWithScore";
    }

    @Override
    protected String getScenarioName() {
        return "idBased";
    }

    @Test(groups = "functional")
    public void execute() throws Exception {
        List<GenericRecord> inputRecords = AvroUtils.readFromLocalFile(ClassLoader
                .getSystemResource(String.format("%s/%s/%s/part-m-00000.avro", //
                        getFlowBeanName(), getScenarioName(), getStandardParameters().getInputTableName())) //
                .getPath());

        executeDataFlow(getStandardParameters());

        List<GenericRecord> outputRecords = readOutput();
        assertEquals(outputRecords.size(), inputRecords.size());
        for (GenericRecord record : outputRecords) {
            assertNotNull(record.get(InterfaceName.Id.name()));
            assertNotNull(record.get(ScoreResultField.Percentile.displayName));
            assertNotNull(record.get(ScoreResultField.Rating.displayName));
        }
    }

    @Override
    protected String getIdColumnName(String tableName) {
        return InterfaceName.Id.name();
    }
}
