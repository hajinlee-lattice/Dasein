package com.latticeengines.cdl.workflow.steps.rating;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;

@Component("createCustomEventScoringTargetTable")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CreateCustomEventScoringTargetTable extends BaseRedshiftIngestStep<GenerateRatingStepConfiguration> {

    @Override
    protected void postIngestion() {
        super.postIngestion();
        removeObjectFromContext(CUSTOM_EVENT_SCORE_FILTER_TABLENAME);
        putStringValueInContext(CUSTOM_EVENT_SCORE_FILTER_TABLENAME, targetTableName);
        putStringValueInContext(SCORING_UNIQUEKEY_COLUMN, InterfaceName.__Composite_Key__.name());
    }

    @Override
    protected Schema generateSchema() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(InterfaceName.__Composite_Key__.name(), String.class));
        columns.add(Pair.of(InterfaceName.AccountId.name(), String.class));
        columns.add(Pair.of(InterfaceName.ModelId.name(), String.class));
        columns.add(Pair.of(MODEL_GUID, String.class));
        columns.add(Pair.of(InterfaceName.CDLUpdatedTime.name(), Long.class));
        return AvroUtils.constructSchema(targetTableName, columns);
    }

    @Override
    protected List<GenericRecord> dataPageToRecords(String modelId, String modelGuid, List<Map<String, Object>> data) {
        List<GenericRecord> records = new ArrayList<>();
        long currentTime = System.currentTimeMillis();
        data.forEach(map -> {
            GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            String accountIdAttr = InterfaceName.AccountId.name();
            try {
                String accountId = (String) map.get(InterfaceName.AccountId.name());
                String compositeKey = String.format("%s_%s", accountId, modelId);
                builder.set(InterfaceName.__Composite_Key__.name(), compositeKey);
                builder.set(accountIdAttr, accountId);
                builder.set(InterfaceName.ModelId.name(), modelId);
                builder.set(MODEL_GUID, modelGuid);
                builder.set(InterfaceName.CDLUpdatedTime.name(), currentTime);
                records.add(builder.build());
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse redshift result: " + map, e);
            }
        });
        return records;
    }

    @Override
    protected RatingEngineType getTargetEngineType() {
        return RatingEngineType.CUSTOM_EVENT;
    }

}
