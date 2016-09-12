package com.latticeengines.propdata.dataflow.refresh;

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
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterSeedFunction;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterSeedRebuildFlowParameter;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn.Calculation;


@Component("accountMasterSeedRebuildFlow")
public class AccountMasterSeedRebuildFlow extends TypesafeDataFlowBuilder<AccountMasterSeedRebuildFlowParameter> {
    private static Logger LOG = LogManager.getLogger(AccountMasterSeedRebuildFlow.class);

    private Map<String, SeedMergeFieldMapping> accountMasterSeedColumnMapping = new HashMap<String, SeedMergeFieldMapping>();
    // dnbCacheSeed columns -> accountMasterSeed columns
    private Map<String, String> dnbCacheSeedColumnMapping = new HashMap<String, String>();
    // latticeCacheSeed columns -> accountMasterSeed columns
    private Map<String, String> latticeCacheSeedColumnMapping = new HashMap<String, String>();


    @Override
    public Node construct(AccountMasterSeedRebuildFlowParameter parameters) {
        try {
            getColumnMapping(parameters.getColumns());
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_25010, e);
        }
        Node dnbCacheSeed = addSource(parameters.getBaseTables().get(0));
        dnbCacheSeed = addRetainDnBColumnNode(dnbCacheSeed);
        Node latticeCacheSeed = addSource(parameters.getBaseTables().get(1));
        latticeCacheSeed = addFilterDomainNode(latticeCacheSeed);
        Node accountMasterSeed = addJoinNode(latticeCacheSeed, dnbCacheSeed, JoinType.OUTER);
        accountMasterSeed = addTmpOutputNode(accountMasterSeed, parameters.getBaseSourcePrimaryKeys().get(0));
        accountMasterSeed = addRetainAccountMasterSeedColumnNode(accountMasterSeed);
        accountMasterSeed = addRenameAccountMasterSeedColumnNode(accountMasterSeed);
        accountMasterSeed = addColumnNode(accountMasterSeed, parameters.getColumns());
        return accountMasterSeed;
    }

    private Node addRetainDnBColumnNode(Node node) {
        List<String> columnNames = new ArrayList<String>(dnbCacheSeedColumnMapping.keySet());
        return node.retain(new FieldList(columnNames));
    }

    private Node addFilterDomainNode(Node node) {
        StringBuilder sb = new StringBuilder();
        List<String> columnNames = new ArrayList<String>();
        for (Map.Entry<String, SeedMergeFieldMapping> entry : accountMasterSeedColumnMapping.entrySet()) {
            SeedMergeFieldMapping item = entry.getValue();
            if (item.getIsDedup()) {
                sb.append(item.getMergedSourceColumn() + " != null && ");
                columnNames.add(item.getMergedSourceColumn());
            }
        }
        if (sb.length() > 0) {
            LOG.info("Filter expression: " + sb.substring(0, sb.length() - 4));
            node.filter(sb.substring(0, sb.length() - 4), new FieldList(columnNames));
        }
        return node;
    }

    private Node addJoinNode(Node latticeCacheSeed, Node dnbCacheSeed, JoinType joinType) {
        List<String> latticeJoinColumns = new ArrayList<String>();
        List<String> dnbJoinColumns = new ArrayList<String>();
        for (Map.Entry<String, SeedMergeFieldMapping> entry : accountMasterSeedColumnMapping.entrySet()) {
            SeedMergeFieldMapping item = entry.getValue();
            if (item.getIsDedup()) {
                latticeJoinColumns.add(item.getMergedSourceColumn());
                dnbJoinColumns.add(item.getMainSourceColumn());
            }
        }
        return latticeCacheSeed.join(new FieldList(latticeJoinColumns), dnbCacheSeed, new FieldList(dnbJoinColumns),
                joinType);
    }

    private Node addTmpOutputNode(Node node, List<String> dnbPrimaryKeys) {
        for (Map.Entry<String, SeedMergeFieldMapping> entry : accountMasterSeedColumnMapping.entrySet()) {
            String outputColumn = "Tmp_" + entry.getKey();
            String latticeColumn = entry.getValue().getMergedSourceColumn();
            String dnbColumn = entry.getValue().getMainSourceColumn();
            List<String> inputColumns = new ArrayList<String>();
            inputColumns.add(latticeColumn);
            inputColumns.add(dnbColumn);
            node = node.apply(new AccountMasterSeedFunction(outputColumn, latticeColumn, dnbColumn, dnbPrimaryKeys),
                    new FieldList(node.getFieldNames()), new FieldMetadata(outputColumn, String.class));
        }
        return node;
    }

    private Node addRetainAccountMasterSeedColumnNode(Node node) {
        List<String> columnNames = new ArrayList<String>();
        for (Map.Entry<String, SeedMergeFieldMapping> entry : accountMasterSeedColumnMapping.entrySet()) {
            columnNames.add("Tmp_" + entry.getKey());
        }
        return node.retain(new FieldList(columnNames));
    }

    private Node addRenameAccountMasterSeedColumnNode(Node node) {
        List<String> newColumnNames = new ArrayList<String>();
        List<String> oldColumnNames = new ArrayList<String>();
        for (Map.Entry<String, SeedMergeFieldMapping> entry : accountMasterSeedColumnMapping.entrySet()) {
            oldColumnNames.add("Tmp_" + entry.getKey());
            newColumnNames.add(entry.getKey());
        }
        return node.rename(new FieldList(oldColumnNames), new FieldList(newColumnNames));
    }

    private Node addColumnNode(Node node, List<SourceColumn> sourceColumns) {
        for (SourceColumn sourceColumn : sourceColumns) {
            switch (sourceColumn.getCalculation()) {
                case ADD_UUID:
                    node = node.addUUID(sourceColumn.getColumnName());
                    break;
                case ADD_TIMESTAMP:
                    node = node.addTimestamp(sourceColumn.getColumnName());
                    break;
                case ADD_ROWNUM:
                    node = node.addRowID(sourceColumn.getColumnName());
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
            if (sourceColumn.getCalculation() == Calculation.MERGE_SEED) {
                ObjectMapper om = new ObjectMapper();
                list = om.readValue(sourceColumn.getArguments(), TypeFactory.defaultInstance()
                        .constructCollectionType(List.class, SeedMergeFieldMapping.class));
                break;
            }
        }

        for (SeedMergeFieldMapping item : list) {
            accountMasterSeedColumnMapping.put(item.getSourceColumn(), item);
            dnbCacheSeedColumnMapping.put(item.getMainSourceColumn(), item.getSourceColumn());
            if (item.getMergedSourceColumn() != null) {
                latticeCacheSeedColumnMapping.put(item.getMergedSourceColumn(), item.getSourceColumn());
            }
        }
    }
}
