package com.latticeengines.cdl.workflow.steps.rating;

import java.util.ArrayList;
import java.util.Collections;
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
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;

@Component("ingestRuleBasedRating")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class IngestRuleBasedRating extends BaseRedshiftIngestStep<GenerateRatingStepConfiguration> {

    private ThreadLocal<Long> ingestTimestamp = new ThreadLocal<>();

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
    protected GenericRecord parseDataForModel(String modelId, String modelGuid, Map<String, Object> data) {
        if (ingestTimestamp.get() == null || ingestTimestamp.get() == 0L) {
            ingestTimestamp.set(System.currentTimeMillis());
        }
        long currentTime = ingestTimestamp.get();
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        String accountIdAttr = InterfaceName.AccountId.name();

        String accountId = (String) data.get(accountIdAttr);
        String compositeKey = String.format("%s_%s", accountId, modelId);
        builder.set(InterfaceName.__Composite_Key__.name(), compositeKey);
        builder.set(accountIdAttr, data.get(accountIdAttr));
        builder.set(InterfaceName.ModelId.name(), modelId);
        builder.set(InterfaceName.Rating.name(), data.get(modelId));
        builder.set(InterfaceName.CDLUpdatedTime.name(), currentTime);
        return builder.build();
    }

    @Override
    protected List<RatingEngineType> getTargetEngineTypes() {
        return Collections.singletonList(RatingEngineType.RULE_BASED);
    }

    @Override
    protected String getTargetTableName() {
        return NamingUtils.timestamp("RuleBased");
    }

}
