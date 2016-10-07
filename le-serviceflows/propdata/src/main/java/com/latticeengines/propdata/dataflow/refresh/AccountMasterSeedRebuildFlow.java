package com.latticeengines.propdata.dataflow.refresh;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterSeedRebuildFlowParameter;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn.Calculation;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;


@Component("accountMasterSeedRebuildFlow")
public class AccountMasterSeedRebuildFlow extends TypesafeDataFlowBuilder<AccountMasterSeedRebuildFlowParameter> {
    private static Logger LOG = LogManager.getLogger(AccountMasterSeedRebuildFlow.class);

    private Map<String, SeedMergeFieldMapping> accountMasterSeedColumnMapping = new HashMap<String, SeedMergeFieldMapping>();
    // dnbCacheSeed columns -> accountMasterSeed columns
    private Map<String, String> dnbCacheSeedColumnMapping = new HashMap<String, String>();
    // latticeCacheSeed columns -> accountMasterSeed columns
    private Map<String, String> latticeCacheSeedColumnMapping = new HashMap<String, String>();
    private String dnbDunsColumn;
    private String leDunsColumn;
    private String dnbDomainColumn;
    private String leDomainColumn;
    private String dnbIsPrimaryDomainColumn;
    private String dnbIsPrimaryLocationColumn;
    private String dnbNumberOfLocationColumn;
    
    /*
        Implement of PD-1196
        A) DnBCacheSeed OUTER JOIN LatticeCacheSeed by DUNS && URL
        1. LE_DUNS = DnB_DUNS (not NULL), LE_URL = DnB_URL (not NULL): 
            Take all the attributes from DnB (Result1)
    
        B) Let LatticeCacheSeed = Result of A with DnB_DUNS NULL && DnB_URL NULL, retain only LatticeCacheSeed columns
           Let DnBCacheSeed = Result of A with LE_DUNS NULL && LE_URL NULL, retain only DnBCacheSeed columns
    
        C) DnBCacheSeed LEFT JOIN LatticeCacheSeed(DUNS+URL) by DUNS
        1. LE_DUNS = DnB_DUNS, DnB_URL NULL:
            Take LE_URL, and all the other attributes from DnB, set IS_PRIMARY_DOMAIN = Y (Result2)
        2. LE_DUNS = DnB_DUNS, LE_URL != DnB_URL:
            Take all the attributes from DnB (Result3)
            Add a new row: Take LE_URL, and all the other attributes from DnB, set IS_PRIMARY_DOMAIN = N (Result4)
        3. LE_DUNS NULL
            Take all the attributes from DnB (Result5)
    
        D) LatticeCacheSeed(URL) LEFT JOIN DnBCacheSeed by URL
        1. DnB_URL NULL
            Take all the attributes from LatticeCacheSeed, set IS_PRIMARY_DOMAIN = Y, IS_PRIMARY_LOCATION = Y (Result6)
            
        E) Merge Result 1, 2, 3, 4, 5, 6
    */


    @Override
    public Node construct(AccountMasterSeedRebuildFlowParameter parameters) {
        try {
            getColumnMapping(parameters.getColumns());
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_25010, e);
        }
        Node dnbCacheSeed = addSource(parameters.getBaseTables().get(0));
        dnbCacheSeed = retainDnBColumns(dnbCacheSeed);
        Node latticeCacheSeed = addSource(parameters.getBaseTables().get(1));
        latticeCacheSeed = retainLeColumns(latticeCacheSeed);

        // Step A
        List<String> dnbJoinFields = new ArrayList<String>();
        dnbJoinFields.add(dnbDunsColumn);
        dnbJoinFields.add(dnbDomainColumn);
        List<String> leJoinFields = new ArrayList<String>();
        leJoinFields.add(leDunsColumn);
        leJoinFields.add(leDomainColumn);
        Node joinedA = join(dnbCacheSeed, dnbJoinFields, latticeCacheSeed, leJoinFields, JoinType.OUTER);
        Node res1 = processA(joinedA);
        
        // Step B
        Node dnbCacheSeedRefined = joinedA.filter(leDunsColumn + " == null && " + leDomainColumn + " == null",
                new FieldList(leDunsColumn, leDomainColumn));
        dnbCacheSeedRefined = retainDnBColumns(dnbCacheSeedRefined);
        dnbCacheSeedRefined = dnbCacheSeedRefined.renamePipe("DnbCacheSeedRefined");
        
        Node latticeCacheSeedDunsDomain = joinedA.filter(
                dnbDunsColumn + " == null && " + dnbDomainColumn + " == null && " + leDunsColumn + " != null && "
                        + leDomainColumn + " != null",
                new FieldList(dnbDunsColumn, leDunsColumn, dnbDomainColumn, leDomainColumn));
        latticeCacheSeedDunsDomain = retainLeColumns(latticeCacheSeedDunsDomain);
        Node latticeCacheSeedOnlyDomain = joinedA.filter(
                dnbDunsColumn + " == null && " + dnbDomainColumn + " == null && " + leDunsColumn + " == null && "
                        + leDomainColumn + " != null",
                new FieldList(dnbDunsColumn, leDunsColumn, dnbDomainColumn, leDomainColumn));
        latticeCacheSeedOnlyDomain = retainLeColumns(latticeCacheSeedOnlyDomain);

        // Step C
        dnbJoinFields = new ArrayList<String>();
        dnbJoinFields.add(dnbDunsColumn);
        leJoinFields = new ArrayList<String>();
        leJoinFields.add(leDunsColumn);
        Node joinedC = join(dnbCacheSeedRefined, dnbJoinFields, latticeCacheSeedDunsDomain, leJoinFields,
                JoinType.LEFT);
        Node joinedCWithLeDuns = joinedC.filter(leDunsColumn + " != null", new FieldList(leDunsColumn));
        Node joinedCWithoutLeDuns = joinedC.filter(leDunsColumn + " == null", new FieldList(leDunsColumn));
        Node res2 = processC1(joinedCWithLeDuns);
        String filterExpression = dnbDomainColumn + " != null && " + leDomainColumn + " != null && !(" + dnbDomainColumn
                + ".equals(" + leDomainColumn + "))";
        Node joinedCWithLeDunsWithDiffDomain = joinedCWithLeDuns.filter(filterExpression,
                new FieldList(dnbDomainColumn, leDomainColumn));
        Node res3 = processC2Part1(joinedCWithLeDunsWithDiffDomain);
        Node res4 = processC2Part2(joinedCWithLeDunsWithDiffDomain);
        Node res5 = processC3(joinedCWithoutLeDuns);

        // Step D
        dnbJoinFields = new ArrayList<String>();
        dnbJoinFields.add(dnbDomainColumn);
        leJoinFields = new ArrayList<String>();
        leJoinFields.add(leDomainColumn);
        Node res6 = join(dnbCacheSeedRefined, dnbJoinFields, latticeCacheSeedOnlyDomain, leJoinFields, JoinType.RIGHT);
        res6 = processD(res6);

        // Step E
        Node accountMasterSeed = res1.merge(res2).merge(res3).merge(res4).merge(res5).merge(res6);

        accountMasterSeed = retainAccountMasterSeedColumnNode(accountMasterSeed);
        accountMasterSeed = renameAccountMasterSeedColumnNode(accountMasterSeed);
        accountMasterSeed = addColumnNode(accountMasterSeed, parameters.getColumns());
        return accountMasterSeed;
    }

    private Node retainDnBColumns(Node node) {
        List<String> columnNames = new ArrayList<String>(dnbCacheSeedColumnMapping.keySet());
        return node.retain(new FieldList(columnNames));
    }

    private Node retainLeColumns(Node node) {
        List<String> columnNames = new ArrayList<String>(latticeCacheSeedColumnMapping.keySet());
        return node.retain(new FieldList(columnNames));
    }

    private Node join(Node leftNode, List<String> leftNodeJoinFields, Node rightNode, List<String> rightNodeJoinFields,
            JoinType joinType) {
        return leftNode.join(new FieldList(leftNodeJoinFields), rightNode, new FieldList(rightNodeJoinFields),
                joinType);
    }

    private Node processA(Node node) {
        String filterExpression = dnbDunsColumn + " != null && " + leDunsColumn + " != null && " + dnbDomainColumn
                + " != null && " + leDomainColumn + " != null";
        Node res = node.filter(filterExpression,
                new FieldList(dnbDunsColumn, leDunsColumn, dnbDomainColumn, leDomainColumn));
        res = callAccountMasterSeedFunction(res, true, new HashSet<String>(), new HashMap<String, Object>());
        return res;
    }

    private Node processC1(Node node) {
        Node res = node.filter(dnbDomainColumn + " == null", new FieldList(dnbDomainColumn));
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(dnbIsPrimaryDomainColumn, "Y");
        Set<String> set = new HashSet<String>();
        set.add(leDomainColumn);
        res = callAccountMasterSeedFunction(res, true, set, map);
        return res;
    }

    private Node processC2Part1(Node node) {
        Node res3 = callAccountMasterSeedFunction(node, true, new HashSet<String>(), new HashMap<String, Object>());
        return res3;
    }

    private Node processC2Part2(Node node) {
        Node res4 = node.groupByAndLimit(new FieldList(leDunsColumn, leDomainColumn), new FieldList(dnbDomainColumn), 1,
                true, true);
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(dnbIsPrimaryDomainColumn, "N");
        Set<String> set = new HashSet<String>();
        set.add(leDomainColumn);
        res4 = callAccountMasterSeedFunction(res4, true, set, map);
        return res4;
    }

    private Node processC3(Node node) {
        Node res5 = callAccountMasterSeedFunction(node, true, new HashSet<String>(), new HashMap<String, Object>());
        return res5;
    }

    private Node processD(Node node) {
        String filterExpression = dnbDomainColumn + " == null";
        Node res = node.filter(filterExpression, new FieldList(dnbDomainColumn));
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(dnbIsPrimaryDomainColumn, "Y");
        map.put(dnbIsPrimaryLocationColumn, "Y");
        map.put(dnbNumberOfLocationColumn, 1);
        res = callAccountMasterSeedFunction(res, false, new HashSet<String>(), map);
        return res;
    }

    private Node callAccountMasterSeedFunction(Node node, boolean takeAllFromDnB, Set<String> exceptColumns,
            Map<String, Object> setDnBColumnValues) {
        for (Map.Entry<String, SeedMergeFieldMapping> entry : accountMasterSeedColumnMapping.entrySet()) {
            String outputColumn = "Tmp_" + entry.getKey();
            String latticeColumn = entry.getValue().getLeColumn();
            String dnbColumn = entry.getValue().getDnbColumn();
            if ((takeAllFromDnB && !exceptColumns.contains(latticeColumn))
                    || (!takeAllFromDnB && exceptColumns.contains(dnbColumn))) {
                node = node.apply(
                        new AccountMasterSeedFunction(outputColumn, dnbColumn, latticeColumn, true,
                                setDnBColumnValues),
                        new FieldList(node.getFieldNames()), new FieldMetadata(outputColumn,
                                entry.getKey().equals(dnbNumberOfLocationColumn) ? Integer.class : String.class));
            } else {
                node = node.apply(
                        new AccountMasterSeedFunction(outputColumn, dnbColumn, latticeColumn, false,
                                setDnBColumnValues),
                        new FieldList(node.getFieldNames()), new FieldMetadata(outputColumn,
                                entry.getKey().equals(dnbNumberOfLocationColumn) ? Integer.class : String.class));
            }
        }
        return node;
    }

    private Node retainAccountMasterSeedColumnNode(Node node) {
        List<String> columnNames = new ArrayList<String>();
        for (Map.Entry<String, SeedMergeFieldMapping> entry : accountMasterSeedColumnMapping.entrySet()) {
            columnNames.add("Tmp_" + entry.getKey());
        }
        return node.retain(new FieldList(columnNames));
    }

    private Node renameAccountMasterSeedColumnNode(Node node) {
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
            accountMasterSeedColumnMapping.put(item.getTargetColumn(), item);
            dnbCacheSeedColumnMapping.put(item.getDnbColumn(), item.getTargetColumn());
            if (item.getLeColumn() != null) {
                latticeCacheSeedColumnMapping.put(item.getLeColumn(), item.getTargetColumn());
            }
            switch (item.getColumnType()) {
                case DOMAIN:
                    dnbDomainColumn = item.getDnbColumn();
                    leDomainColumn = item.getLeColumn();
                    break;
                case DUNS:
                    dnbDunsColumn = item.getDnbColumn();
                    leDunsColumn = item.getLeColumn();
                    break;
                case IS_PRIMARY_DOMAIN:
                    dnbIsPrimaryDomainColumn = item.getDnbColumn();
                    break;
                case IS_PRIMARY_LOCATION:
                    dnbIsPrimaryLocationColumn = item.getDnbColumn();
                    break;
                case NUMBER_OF_LOCATION:
                    dnbNumberOfLocationColumn = item.getDnbColumn();
                    break;
                default:
                    break;
            }
        }
    }
}
