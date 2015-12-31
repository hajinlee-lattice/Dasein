package com.latticeengines.propdata.collection.service.impl;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotStrategyImpl;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotType;
import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.entitymanager.RefreshProgressEntityMgr;
import com.latticeengines.propdata.collection.service.PivotService;
import com.latticeengines.propdata.collection.source.PivotedSource;
import com.latticeengines.propdata.collection.source.impl.FeaturePivoted;

@Component("featurePivotService")
public class FeaturePivotService extends AbstractPivotService implements PivotService {

    Log log = LogFactory.getLog(this.getClass());

    @Autowired
    ArchiveProgressEntityMgr archiveProgressEntityMgr;

    @Autowired
    RefreshProgressEntityMgr progressEntityMgr;

    @Autowired
    FeaturePivoted source;

    @Override
    public PivotedSource getSource() { return source; }

    @Override
    RefreshProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    Log getLogger() { return log; }

    @Override
    DataFlowBuilder.FieldList getGroupByFields() {
        return new DataFlowBuilder.FieldList("URL", "Feature");
    }

    @Override
    PivotStrategyImpl getPivotStrategy() {
        List<Map<String, Object>> results = jdbcTemplateCollectionDB.queryForList(
                "SELECT [Feature], [ColumnType] FROM [FeaturePivotMapping]");

        String keyColumn = "Feature";
        String valueColumn = "Value";

        Set<String> pivotedKeys = new HashSet<>();
        Map<String, Class<?>> resultColumnClassMap = new HashMap<>();
        Map<String, Serializable> defaultValues = new HashMap<>();

        for (Map<String, Object> result: results) {
            String feature = (String) result.get("Feature");
            String type = (String) result.get("ColumnType");
            pivotedKeys.add(feature);
            if ("INT".equalsIgnoreCase(type)) {
                resultColumnClassMap.put(feature, Integer.class);
                defaultValues.put(feature, 0);
            } else {
                resultColumnClassMap.put(feature, String.class);
                defaultValues.put(feature, null);
            }
        }

        return new PivotStrategyImpl(keyColumn, valueColumn, pivotedKeys,
                null,
                resultColumnClassMap, String.class,
                null, PivotType.ANY,
                defaultValues, 1);
    }

    @Override
    protected void createStageTable() {
        super.createStageTable();
        jdbcTemplateCollectionDB.execute(
                "CREATE CLUSTERED INDEX IX_URLFeature ON [" + getStageTableName() + "] ([URL])");
        jdbcTemplateCollectionDB.execute(
                "CREATE INDEX IX_Timtstamp ON [" + getStageTableName() + "] ([Timestamp])");
    }


}
