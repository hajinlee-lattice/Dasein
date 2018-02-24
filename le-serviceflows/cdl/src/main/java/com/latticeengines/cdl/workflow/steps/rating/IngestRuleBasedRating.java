package com.latticeengines.cdl.workflow.steps.rating;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;

@Component("ingestRuleBasedRating")
public class IngestRuleBasedRating extends BaseRedshiftIngestStep<GenerateRatingStepConfiguration> {

    protected void postIngestion() {
        super.postIngestion();
        putStringValueInContext(RULE_RAW_RATING_TABLE_NAME, targetTableName);
    }

    @Override
    protected Schema generateSchema() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(InterfaceName.__Composite_Key__.name(), String.class));
        columns.add(Pair.of(InterfaceName.AccountId.name(), String.class));
        columns.add(Pair.of(InterfaceName.ModelId.name(), String.class));
        columns.add(Pair.of(InterfaceName.Rating.name(), String.class));
        columns.add(Pair.of(InterfaceName.CDLUpdatedTime.name(), Long.class));
        return AvroUtils.constructSchema(targetTableName, columns);
    }

    @Override
    protected List<GenericRecord> dataPageToRecords(String modelId, String modelGuid, List<Map<String, Object>> data) {
        List<GenericRecord> records = new ArrayList<>();
        data.forEach(map -> {
            long currentTime = System.currentTimeMillis();
            GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            String accountIdAttr = InterfaceName.AccountId.name();

            String accountId = (String) map.get(accountIdAttr);
            String compositeKey = String.format("%s_%s", accountId, modelId);
            builder.set(InterfaceName.__Composite_Key__.name(), compositeKey);
            builder.set(accountIdAttr, map.get(accountIdAttr));
            builder.set(InterfaceName.ModelId.name(), modelId);
            builder.set(InterfaceName.Rating.name(), map.get(modelId));
            builder.set(InterfaceName.CDLUpdatedTime.name(), currentTime);
            records.add(builder.build());
        });
        return records;
    }

    @Override
    protected RatingEngineType getTargetEngineType() {
        return RatingEngineType.RULE_BASED;
    }

}
