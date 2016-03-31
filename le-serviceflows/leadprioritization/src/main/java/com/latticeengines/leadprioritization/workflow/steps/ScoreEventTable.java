package com.latticeengines.leadprioritization.workflow.steps;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.serviceflows.workflow.scoring.Score;

public class ScoreEventTable extends Score {
    @Override
    public void onConfigurationInitialized() {
        Table table = JsonUtils.deserialize(executionContext.getString(EVENT_TABLE), Table.class);
        executionContext.putString(SCORING_SOURCE_DIR, table.getExtractsDirectory());
    }
}