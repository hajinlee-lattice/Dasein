package com.latticeengines.cdl.workflow.steps.rating;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;

@Component("createScoringTargetTable")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CreateScoringTargetTable extends BaseRedshiftIngestStep<GenerateRatingStepConfiguration> {

    private AtomicLong evaluationPeriod = null;

    private Map<String, RatingEngineType> ratingEngineTypeMap = new HashMap<>();
    private boolean hasCrossSellModel = false;

    @Override
    protected void preIngestion() {
        super.preIngestion();
        containers.forEach(container -> {
            String modelGuid = ((AIModel) container.getModel()).getModelSummaryId();
            RatingEngineType ratingEngineType = container.getEngineSummary().getType();
            ratingEngineTypeMap.put(modelGuid, ratingEngineType);
            if (!hasCrossSellModel && RatingEngineType.CROSS_SELL.equals(ratingEngineType)) {
                hasCrossSellModel = true;
            }
        });
    }

    @Override
    protected void postIngestion() {
        super.postIngestion();
        removeObjectFromContext(FILTER_EVENT_TABLE);
        putStringValueInContext(FILTER_EVENT_TARGET_TABLE_NAME, targetTableName);
        putStringValueInContext(SCORING_UNIQUEKEY_COLUMN, InterfaceName.__Composite_Key__.name());
        if (evaluationPeriod != null)
            putLongValueInContext(EVALUATION_PERIOD, evaluationPeriod.get());
    }

    @Override
    protected Schema generateSchema() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(InterfaceName.__Composite_Key__.name(), String.class));
        columns.add(Pair.of(InterfaceName.AccountId.name(), String.class));
        columns.add(Pair.of(InterfaceName.PeriodId.name(), Long.class));
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
            RatingEngineType ratingEngineType = ratingEngineTypeMap.get(modelGuid);
            try {
                String accountId = (String) map.get(accountIdAttr.toLowerCase());
                String compositeKey = String.format("%s_%s", accountId, modelId);
                builder.set(InterfaceName.__Composite_Key__.name(), compositeKey);
                builder.set(accountIdAttr, map.get(accountIdAttr.toLowerCase()));
                builder.set(InterfaceName.ModelId.name(), modelId);
                builder.set(MODEL_GUID, modelGuid);
                builder.set(InterfaceName.CDLUpdatedTime.name(), currentTime);

                String periodIdAttr = InterfaceName.PeriodId.name();
                if (RatingEngineType.CROSS_SELL.equals(ratingEngineType)) {
                    if (evaluationPeriod == null) {
                        Long periodId = Long
                                .valueOf(String.valueOf(map.get(InterfaceName.PeriodId.name().toLowerCase())));
                        evaluationPeriod = new AtomicLong();
                        evaluationPeriod.set(periodId);
                    }
                    builder.set(periodIdAttr, evaluationPeriod.get());
                } else {
                    builder.set(periodIdAttr, null);
                }

                records.add(builder.build());
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse redshift result: " + map, e);
            }
        });
        return records;
    }

    @Override
    protected List<RatingEngineType> getTargetEngineTypes() {
        return Arrays.asList(RatingEngineType.CROSS_SELL, RatingEngineType.CUSTOM_EVENT);
    }

    @Override
    protected String getTargetTableName() {
        return NamingUtils.timestamp("ScoringTarget");
    }

}
