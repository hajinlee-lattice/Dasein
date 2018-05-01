package com.latticeengines.scoring.dataflow;

import static org.testng.Assert.assertNotNull;

import java.util.List;

import com.latticeengines.domain.exposed.util.BucketMetadataUtils;
import org.apache.avro.generic.GenericRecord;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CombineInputTableWithScoreParameters;
import com.latticeengines.domain.exposed.util.BucketMetadataUtils;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-scoring-dataflow-context.xml" })
public class CombineInputTableWithScoreMultiModelTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    private CombineInputTableWithScoreParameters getStandardParameters() {
        CombineInputTableWithScoreParameters params = new CombineInputTableWithScoreParameters("ScoreResult",
                "InputTable");
        params.setIdColumn(InterfaceName.__Composite_Key__.name());
        params.setModelIdField(ScoreResultField.ModelId.displayName);
        params.setBucketMetadataMap(ImmutableMap.of( //
                "ms__6e6f1ad0-8ca5-4102-8477-0b9c79cca206-ai_6rvlw", getDefaultBucketMetadata(), //
                "ms__c0b0a2f0-8fd4-4817-aee6-fc47c8e6745b-ai_qr7cx", getDefaultBucketMetadata(), //
                "ms__308f62f0-addb-4dae-8a81-7ad2c55d5618-ai_c3lyq", getDefaultBucketMetadata(), //
                "ms__05e1fdff-69a4-4337-baee-c66cb315ee0a-ai_1mgui", getDefaultBucketMetadata() //

        ));
        return params;
    }

    @Override
    protected String getFlowBeanName() {
        return "combineInputTableWithScore";
    }

    @Override
    protected String getScenarioName() {
        return "multiModel";
    }

    @Test(groups = "functional")
    public void execute() {
        CombineInputTableWithScoreParameters params = getStandardParameters();
        executeDataFlow(params);
        List<GenericRecord> outputRecords = readOutput();
        for (GenericRecord record : outputRecords) {
            System.out.println(record);
            assertNotNull(record.get(ScoreResultField.Rating.displayName));
        }
    }

    private List<BucketMetadata> getDefaultBucketMetadata() {
        return BucketMetadataUtils.getDefaultMetadata();
    }

    @Override
    protected String getIdColumnName(String tableName) {
        return InterfaceName.__Composite_Key__.name();
    }
}
