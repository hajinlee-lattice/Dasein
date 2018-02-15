package com.latticeengines.scoring.workflow.steps;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.ScoreStepConfiguration;

@Component("scoreEventTable")
public class ScoreEventTable extends BaseScoreStep<ScoreStepConfiguration> {

    public ScoreEventTable() {
    }

    @Override
    public void onConfigurationInitialized() {
        configuration.setRegisterScoredTable(true);
        Table table = getObjectFromContext(EVENT_TABLE, Table.class);
        putStringValueInContext(SCORING_SOURCE_DIR,
                StringUtils.substringBeforeLast(table.getExtracts().get(0).getPath(), "*.avro"));

        Attribute id = null;
        PrimaryKey primaryKey = table.getPrimaryKey();
        if (primaryKey == null) {
            List<Attribute> idAttrs = table.getAttributes(LogicalDataType.Id);
            if (CollectionUtils.isEmpty(idAttrs)) {
                idAttrs = table.getAttributes(LogicalDataType.InternalId);
            }
            if (CollectionUtils.isNotEmpty(idAttrs)) {
                id = idAttrs.get(0);
            }
        } else {
            id = table.getAttribute(primaryKey.getAttributes().get(0));
        }
        if (id == null) {
            throw new RuntimeException(String.format("Could not locate unique key to use to score table %s: %s",
                    table.getName(), JsonUtils.pprint(table)));
        }

        putStringValueInContext(SCORING_UNIQUEKEY_COLUMN, id.getName());
    }
}