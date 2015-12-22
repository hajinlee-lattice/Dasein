package com.latticeengines.propdata.collection.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.pivot.PivotMapper;
import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.entitymanager.PivotProgressEntityMgr;
import com.latticeengines.propdata.collection.service.PivotService;
import com.latticeengines.propdata.collection.source.PivotedSource;

@Component("featurePivotService")
public class FeaturePivotServiceImpl extends AbstractPivotService implements PivotService {

    Log log = LogFactory.getLog(this.getClass());

    @Autowired
    ArchiveProgressEntityMgr archiveProgressEntityMgr;

    @Autowired
    PivotProgressEntityMgr progressEntityMgr;

    @Override
    public PivotedSource getSource() { return PivotedSource.FEATURE_PIVOTED; }

    @Override
    PivotProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    ArchiveProgressEntityMgr getBaseSourceArchiveProgressEntityMgr() { return archiveProgressEntityMgr; }

    @Override
    Log getLogger() { return log; }

    @Override
    DataFlowBuilder.FieldList getGroupByFields() {
        return new DataFlowBuilder.FieldList("URL", "Feature");
    }

    @Override
    PivotMapper getPivotMapper() {
        List<Map<String, Object>> results = jdbcTemplateCollectionDB.queryForList(
                "SELECT [Feature], [Result_Column_Type] FROM [FeatureManagement_FeaturePivot]");

        String keyColumn = "Feature";
        String valueColumn = "Value";

        Set<String> pivotedKeys = new HashSet<>();
        Map<String, Class<?>> resultColumnClassMap = new HashMap<>();
        Map<String, Object> defaultValues = new HashMap<>();

        for (Map<String, Object> result: results) {
            String feature = (String) result.get("Feature");
            String type = (String) result.get("Result_Column_Type");
            pivotedKeys.add(feature);
            if ("INT".equalsIgnoreCase(type)) {
                resultColumnClassMap.put(feature, Integer.class);
                defaultValues.put(feature, 0);
            } else {
                resultColumnClassMap.put(feature, String.class);
                defaultValues.put(feature, null);
            }
        }

        return new PivotMapper(keyColumn, valueColumn, pivotedKeys, String.class, null, null,
                resultColumnClassMap, defaultValues, 1);
    }

    @Override
    protected String createStageTableSql() {
        List<Map<String, Object>> results = jdbcTemplateCollectionDB.queryForList(
                "SELECT [Feature], [Result_Column_Type] FROM [FeatureManagement_FeaturePivot]");

        String sql = "CREATE TABLE [" + getStageTableName() +" ] ( \n";
        sql += "[URL] NVARCHAR(500) NOT NULL, \n";

        List<String> columns = new ArrayList<>();
        for (Map<String, Object> result: results) {
            String feature = (String) result.get("Feature");
            String type = (String) result.get("Result_Column_Type");
            columns.add("[" + feature + "] " + type + " NULL");
        }
        sql += StringUtils.join(columns, ", \n");
        sql += " ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY] \n";

        sql += "CREATE CLUSTERED INDEX IX_URLFeature ON [Feature_Pivoted_Source_stage] ([URL])";

        return sql;
    }


}
