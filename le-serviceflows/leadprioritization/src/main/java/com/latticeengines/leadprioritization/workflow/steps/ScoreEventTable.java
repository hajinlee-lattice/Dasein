package com.latticeengines.leadprioritization.workflow.steps;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.serviceflows.workflow.scoring.BaseScoreStep;
import com.latticeengines.serviceflows.workflow.scoring.ScoreStepConfiguration;

@Component("scoreEventTable")
public class ScoreEventTable extends BaseScoreStep<ScoreStepConfiguration> {

    public ScoreEventTable() {
    }

    @Override
    public void onConfigurationInitialized() {
        configuration.setRegisterScoredTable(true);
        Table table = JsonUtils.deserialize(executionContext.getString(EVENT_TABLE), Table.class);
        executionContext.putString(SCORING_SOURCE_DIR, table.getExtracts().get(0).getPath());

        Attribute id;
        PrimaryKey primaryKey = table.getPrimaryKey();
        if (primaryKey == null) {
            id = table.getAttributes(LogicalDataType.Id).get(0);
        } else {
            id = table.getAttribute(primaryKey.getAttributes().get(0));
        }
        if (id == null) {
            throw new RuntimeException(String.format("Could not locate unique key to use to score table %s",
                    table.getName()));
        }

        executionContext.putString(SCORING_UNIQUEKEY_COLUMN, id.getName());
    }
}