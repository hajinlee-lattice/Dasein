package com.latticeengines.cdl.workflow.steps.rating;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;

@Component("createScoringTargetTable")
public class CreateScoringTargetTable extends BaseRedshiftIngestStep<GenerateRatingStepConfiguration> {

    private static final String MODEL_GUID = "Model_GUID";

    @Override
    protected void postIngestion() {
        super.postIngestion();
        putStringValueInContext(FILTER_EVENT_TARGET_TABLE_NAME, targetTableName);
        putStringValueInContext(SCORING_UNIQUEKEY_COLUMN, InterfaceName.__Composite_Key__.name());
        String modelIds = StringUtils.join(containers.stream().map(container -> {
            AIModel aiModel = (AIModel) container.getModel();
            String modelSummaryId = aiModel.getModelSummary().getId();
            if (StringUtils.isBlank(modelSummaryId)) {
                throw new RuntimeException("Found an empty model summary id in AI model: " + JsonUtils.serialize(aiModel));
            } else {
               return modelSummaryId;
            }
        }).collect(Collectors.toList()), "|");
        putStringValueInContext(SCORING_MODEL_ID, modelIds);
    }

    @Override
    protected Schema generateSchema() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(InterfaceName.__Composite_Key__.name(), String.class));
        columns.add(Pair.of(InterfaceName.AccountId.name(), String.class));
        columns.add(Pair.of(InterfaceName.PeriodId.name(), Long.class));
        columns.add(Pair.of(InterfaceName.ModelId.name(), String.class));
        columns.add(Pair.of(MODEL_GUID, String.class));
        columns.add(Pair.of(InterfaceName.__Revenue.name(), Double.class));
        columns.add(Pair.of(InterfaceName.CDLUpdatedTime.name(), Long.class));
        return AvroUtils.constructSchema(targetTableName, columns);
    }

    @Override
    protected List<GenericRecord> dataPageToRecords(String modelId, String modelGuid, List<Map<String, Object>> data) {
        List<GenericRecord> records = new ArrayList<>();
        long currentTime = System.currentTimeMillis();
        data.forEach(map -> {
            System.out.println(JsonUtils.pprint(map));
            GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            String accountIdAttr = InterfaceName.AccountId.name();
            String periodIdAttr = InterfaceName.PeriodId.name();
            String revenue = InterfaceName.__Revenue.name().substring(2).toLowerCase();
            try {
                String accountId = (String) map.get(accountIdAttr.toLowerCase());
                Long periodId = Long.valueOf(String.valueOf(map.get(periodIdAttr.toLowerCase())));
                String compositeKey = String.format("%s_%s", accountId, modelId);
                builder.set(InterfaceName.__Composite_Key__.name(), compositeKey);
                builder.set(accountIdAttr, map.get(accountIdAttr.toLowerCase()));
                builder.set(periodIdAttr, periodId);
                builder.set(InterfaceName.ModelId.name(), modelId);
                builder.set(MODEL_GUID, modelGuid);
                if (map.containsKey(revenue)) {
                    builder.set(InterfaceName.__Revenue.name(), Double.valueOf(String.valueOf(map.get(revenue))));
                } else {
                    builder.set(InterfaceName.__Revenue.name(), null);
                }
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
        return RatingEngineType.AI_BASED;
    }

}
