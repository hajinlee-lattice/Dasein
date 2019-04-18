package com.latticeengines.scoring.workflow.steps;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.scoring.ScoringConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.ScoreStepConfiguration;

@Component("scoreEventTable")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ScoreEventTable extends BaseScoreStep<ScoreStepConfiguration> {

    @Override
    public void onConfigurationInitialized() {
        configuration.setRegisterScoredTable(true);
        Table table = getObjectFromContext(EVENT_TABLE, Table.class);
        putStringValueInContext(SCORING_SOURCE_DIR,
                StringUtils.substringBeforeLast(table.getExtracts().get(0).getPath(), "*.avro"));
        if (StringUtils.isBlank(getStringValueFromContext(SCORING_UNIQUEKEY_COLUMN))) {
            Attribute id = resolveIdAttr(table);
            putStringValueInContext(SCORING_UNIQUEKEY_COLUMN, id.getName());
        }
    }

    private Attribute resolveIdAttr(Table eventTable) {
        Attribute id = null;
        List<Attribute> idAttrs = eventTable.getAttributes(LogicalDataType.InternalId);
        if (!CollectionUtils.isEmpty(idAttrs)) {
            id = idAttrs.get(0);
        } else {
            PrimaryKey primaryKey = eventTable.getPrimaryKey();
            if (primaryKey == null) {
                idAttrs = eventTable.getAttributes(LogicalDataType.Id);
                if (CollectionUtils.isNotEmpty(idAttrs)) {
                    id = idAttrs.get(0);
                }
            } else {
                id = eventTable.getAttribute(primaryKey.getAttributes().get(0));
            }
        }
        if (id == null) {
            throw new RuntimeException(String.format("Could not locate unique key to use to score table %s: %s",
                    eventTable.getName(), JsonUtils.pprint(eventTable)));
        }
        return id;
    }

    @Override
    protected ScoringConfiguration.ScoringInputType getScoringInputType() {
        return ScoringConfiguration.ScoringInputType.Json;
    }
}
