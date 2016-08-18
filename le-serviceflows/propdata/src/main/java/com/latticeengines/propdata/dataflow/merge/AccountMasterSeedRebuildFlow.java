package com.latticeengines.propdata.dataflow.merge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.CopyValueBetweenColumnFunction;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.dataflow.RebuildFlowParameter;
import com.latticeengines.domain.exposed.propdata.manage.SourceColumn;
import com.latticeengines.domain.exposed.propdata.manage.SourceColumn.Calculation;


@Component("accountMasterSeedRebuildFlow")
public class AccountMasterSeedRebuildFlow extends TypesafeDataFlowBuilder<RebuildFlowParameter> {
    private static Logger LOG = LogManager.getLogger(AccountMasterSeedRebuildFlow.class);

    // Based on assumption that main base source has all the output columns
    // (probably with different names)
    private Map<String, SeedMergeFieldMapping> outputColumnMapping = new HashMap<String, SeedMergeFieldMapping>();
    // mainBaseSourceColumn -> outputColumn
    private Map<String, String> mainBaseSourceColumnMapping = new HashMap<String, String>();
    // mergedBaseSourceColumn -> mainBaseSourceColumn
    private Map<String, String> mergedBaseSourceColumnMapping = new HashMap<String, String>();


    @Override
    public Node construct(RebuildFlowParameter parameters) {
        try {
            getColumnMapping(parameters.getColumns());
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_25010, e);
        }
        Node mainBaseSource = addSource(parameters.getBaseTables().get(0));
        mainBaseSource = addRetainNode(mainBaseSource, mainBaseSourceColumnMapping);
        mainBaseSource = addRenameNode(mainBaseSource, mainBaseSourceColumnMapping);
        mainBaseSource = addColumnNode(mainBaseSource, parameters.getColumns());

        /*
        Node mergedBaseSource = addSource(parameters.getBaseTables().get(1));
        mergedBaseSource = addRetainNode(mergedBaseSource, mergedBaseSourceColumnMapping);
        mergedBaseSource = addJoinNode(mergedBaseSource, mainBaseSource, outputColumnMapping,
                JoinType.LEFT);
        mergedBaseSource = addFilterNode(mergedBaseSource, outputColumnMapping);
        mergedBaseSource = addPopulateMainBaseSourceColumnNode(mergedBaseSource,
                mergedBaseSourceColumnMapping);
        mergedBaseSource = addRetainNode(mergedBaseSource, mainBaseSourceColumnMapping);

        Node joinedBaseSource = addJoinNode(mergedBaseSource, mainBaseSource,
                outputColumnMapping, JoinType.OUTER);
        Node joinedBaseSourceMainPart = addGetMainBaseSourcePartNode(joinedBaseSource,
                parameters.getBaseSourcePrimaryKeys().get(0));
        joinedBaseSourceMainPart = addRetainNode(joinedBaseSourceMainPart,
                mainBaseSourceColumnMapping);
        Node joinedBaseSourceMergedPart = addFilterOutMainBaseSourcePartNode(joinedBaseSource,
                parameters.getBaseSourcePrimaryKeys().get(0));
        joinedBaseSourceMergedPart = addRenameMergedNode(joinedBaseSourceMergedPart,
                mergedBaseSourceColumnMapping);
        joinedBaseSourceMergedPart = addRetainNode(joinedBaseSourceMergedPart,
                mainBaseSourceColumnMapping);
        joinedBaseSourceMainPart = joinedBaseSourceMainPart.merge(joinedBaseSourceMergedPart);
        joinedBaseSourceMainPart = addRenameNode(joinedBaseSourceMainPart,
                mainBaseSourceColumnMapping);
                */
        /*
        mergedBaseSource = addFilterNode(mergedBaseSource, outputColumnMapping);
        mergedBaseSource = addRenameMergedNode(mergedBaseSource, mergedBaseSourceColumnMapping);
        mergedBaseSource = addRetainNode(mergedBaseSource, mainBaseSourceColumnMapping);
        mainBaseSource.merge(mergedBaseSource);
        mainBaseSource = addRenameNode(mainBaseSource, mainBaseSourceColumnMapping);
        */

        return mainBaseSource;
    }

    private Node addRetainNode(Node node, Map<String, String> baseSourceColumnMapping) {
        List<String> columnNames = new ArrayList<String>(baseSourceColumnMapping.keySet());
        return node.retain(new FieldList(columnNames));
    }

    private Node addJoinNode(Node lhs, Node rhs,
            Map<String, SeedMergeFieldMapping> outputColumnMapping, JoinType joinType) {
        List<String> lhsJoinColumnNames = new ArrayList<String>();
        List<String> rhsJoinColumnNames = new ArrayList<String>();
        for (Map.Entry<String, SeedMergeFieldMapping> entry : outputColumnMapping.entrySet()) {
            SeedMergeFieldMapping item = entry.getValue();
            if (item.getIsDedup()) {
                lhsJoinColumnNames.add(item.getMergedSourceColumn());
                rhsJoinColumnNames.add(item.getMainSourceColumn());
            }
        }
        return lhs.join(new FieldList(lhsJoinColumnNames), rhs, new FieldList(rhsJoinColumnNames),
                joinType);
    }

    private Node addRenameNode(Node node, Map<String, String> mainBaseSourceColumnMapping) {
        List<String> newColumnNames = new ArrayList<String>(mainBaseSourceColumnMapping.values());
        List<String> oldColumnNames = new ArrayList<String>(mainBaseSourceColumnMapping.keySet());
        return node.rename(new FieldList(oldColumnNames), new FieldList(newColumnNames));
    }

    private Node addFilterNode(Node node,
            Map<String, SeedMergeFieldMapping> outputColumnMapping) {
        List<String> columnNames = new ArrayList<String>();
        for (Map.Entry<String, SeedMergeFieldMapping> entry : outputColumnMapping.entrySet()) {
            SeedMergeFieldMapping item = entry.getValue();
            if (item.getIsDedup()) {
                columnNames.add(item.getMainSourceColumn());
            }
        }
        StringBuilder sb = new StringBuilder();
        for (String columnName : columnNames) {
            sb.append(columnName + " == null && ");
        }
        LOG.info("Filter Expression: " + sb.substring(0, sb.length() - 4));
        if (sb.length() > 0) {
            return node.filter(sb.substring(0, sb.length() - 4), new FieldList(columnNames));
        } else {
            return node;
        }
    }

    private Node addPopulateMainBaseSourceColumnNode(Node node,
            Map<String, String> mergedBaseSourceColumnMapping) {
        List<String> inputColumnNames = new ArrayList<String>(
                mergedBaseSourceColumnMapping.keySet());
        List<String> outputColumnNames = new ArrayList<String>(
                mergedBaseSourceColumnMapping.values());
        String[] outputColumnArr = (String[]) outputColumnNames.toArray();
        List<FieldMetadata> outputColumnMetaDatas = new ArrayList<FieldMetadata>();
        for (String columnName : outputColumnNames) {
            outputColumnMetaDatas.add(new FieldMetadata(columnName, String.class));
        }
        return node.apply(
                new CopyValueBetweenColumnFunction(mergedBaseSourceColumnMapping, outputColumnArr),
                new FieldList(inputColumnNames), outputColumnMetaDatas,
                new FieldList(outputColumnNames));
    }

    private Node addColumnNode(Node node, List<SourceColumn> sourceColumns) {
        for (SourceColumn sourceColumn : sourceColumns) {
            switch (sourceColumn.getCalculation()) {
                case UUID:
                    node = node.addUUID(sourceColumn.getColumnName());
                    break;
                case TIMESTAMP:
                    node = node.addTimestamp(sourceColumn.getColumnName());
                    break;
                default:
                    break;
            }
        }
        return node;
    }

    private void getColumnMapping(List<SourceColumn> sourceColumns)
            throws JsonParseException, JsonMappingException, IOException {
        List<SeedMergeFieldMapping> list = null;
        for (SourceColumn sourceColumn : sourceColumns) {
            if (sourceColumn.getCalculation() == Calculation.SEED_MERGE) {
                ObjectMapper om = new ObjectMapper();
                list = om.readValue(sourceColumn.getArguments(), TypeFactory.defaultInstance()
                        .constructCollectionType(List.class, SeedMergeFieldMapping.class));
                break;
            }
        }

        for (SeedMergeFieldMapping item : list) {
            outputColumnMapping.put(item.getSourceColumn(), item);
            mainBaseSourceColumnMapping.put(item.getMainSourceColumn(), item.getSourceColumn());
            if (item.getMergedSourceColumn() != null) {
                mergedBaseSourceColumnMapping.put(item.getMergedSourceColumn(),
                        item.getMainSourceColumn());
            }
        }
    }
}
