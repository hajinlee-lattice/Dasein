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
    private AccountMasterSeedRebuildFlowParameter parameters;
    
    /*
        The detailed description of implementation of PD-1196 to build AccountMasterSeed is at the bottom of this java file 
    */


    @Override
    public Node construct(AccountMasterSeedRebuildFlowParameter parameters) {
        try {
            this.parameters = parameters;
            getColumnMapping(parameters.getColumns());
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_25010, e);
        }
        Node dnb = addSource(parameters.getBaseTables().get(0));
        dnb = retainDnBColumns(dnb);
        Node le = addSource(parameters.getBaseTables().get(1));
        le = retainLeColumns(le);

        // Step A
        List<String> dnbJoinFields = new ArrayList<String>();
        dnbJoinFields.add(dnbDunsColumn);
        dnbJoinFields.add(dnbDomainColumn);
        List<String> leJoinFields = new ArrayList<String>();
        leJoinFields.add(leDunsColumn);
        leJoinFields.add(leDomainColumn);
        Node joinedA = join(dnb, dnbJoinFields, le, leJoinFields, JoinType.OUTER);
        Node res1 = processA(joinedA); // Step A.1
        
        // Step B
        Node dnbRefined = joinedA.filter(leDunsColumn + " == null && " + leDomainColumn + " == null",
                new FieldList(leDunsColumn, leDomainColumn));
        dnbRefined = retainDnBColumns(dnbRefined);
        dnbRefined = dnbRefined.renamePipe("DnbCacheSeedRefined");
        
        Node leDunsDomain = joinedA.filter(
                dnbDunsColumn + " == null && " + dnbDomainColumn + " == null && " + leDunsColumn + " != null && "
                        + leDomainColumn + " != null",
                new FieldList(dnbDunsColumn, leDunsColumn, dnbDomainColumn, leDomainColumn));
        leDunsDomain = retainLeColumns(leDunsDomain);
        Node leOnlyDomain = joinedA.filter(
                dnbDunsColumn + " == null && " + dnbDomainColumn + " == null && " + leDunsColumn + " == null && "
                        + leDomainColumn + " != null",
                new FieldList(dnbDunsColumn, leDunsColumn, dnbDomainColumn, leDomainColumn));
        leOnlyDomain = retainLeColumns(leOnlyDomain);

        // Step C
        dnbJoinFields = new ArrayList<String>();
        dnbJoinFields.add(dnbDunsColumn);
        leJoinFields = new ArrayList<String>();
        leJoinFields.add(leDunsColumn);
        Node joinedC = join(dnbRefined, dnbJoinFields, leDunsDomain, leJoinFields,
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
        Node res6 = join(dnbRefined, dnbJoinFields, leOnlyDomain, leJoinFields, JoinType.RIGHT);
        res6 = processD(res6);

        // Step E
        Node accountMasterSeed = res1.merge(res2).merge(res3).merge(res4).merge(res5).merge(res6);
        /*
        accountMasterSeed = retainAccountMasterSeedColumnNode(accountMasterSeed);
        accountMasterSeed = renameAccountMasterSeedColumnNode(accountMasterSeed);
        accountMasterSeed = addColumnNode(accountMasterSeed, parameters.getColumns());
        */
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
        node = callAccountMasterSeedFunction(node, true, new HashSet<String>(), new HashMap<String, Object>());
        node = retainAccountMasterSeedColumnNode(node);
        node = renameAccountMasterSeedColumnNode(node);
        node = addColumnNode(node, parameters.getColumns());
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

    /*
        Implementation for PD-1196
        Preprocessing on DnBCacheSeed (in dataflow to generate DnBCacheSeed):
        1. Removed all the rows without DUNS (Actually there is no row without DUNS)
        2. Pair of DUNS && URL is unique
        
        Preprocessing on LatticeCacheSeed (in SP CacheSeedManagement_CreateLatticeCacheSeed_Final of sql script file CacheSeedManagement_Scripts):
        1. Removed all the rows without URL
        2. Pair of DUNS && URL is unique
        
        A) Node joinedA = dnb OUTER JOIN le by DUNS && URL
        1. Node res1 = Select rows from joinedA with LE_DUNS = DnB_DUNS (not NULL), LE_URL = DnB_URL (not NULL) 
                   and choose values for name/location/etc from DnB columns
            (Req 3.A if DUNS x URL exists - do nothing)
        
        B) Node dnbRefined = Select rows from joinedA with LE_DUNS NULL && LE_URL NULL, retain only DnB columns
           Node leDunsDomain = Select rows from joinedA with DnB_DUNS NULL && DnB_URL NULL && LE_DUNS not NULL && LE_URL not NULL, retain only LE columns
           Node leOnlyDomain = Select rows from joinedA with DnB_DUNS NULL && DnB_URL NULL && LE_DUNS NULL && LE_URL not NULL, retain only LE columns
        
        C) Node joinedC = dnbRefined LEFT JOIN leDunsDomain by DUNS
           Node joinedCWithLeDuns = Select rows from joinedC with LE_DUNS not NULL
           Node joinedCWithoutLeDuns = Select rows from joinedC with LE_DUNS NULL
        1. Node res2 = Select rows from joinedCWithLeDuns with DnB_URL NULL
                   and choose LE_URL, set IS_PRIMARY_DOMAIN = Y and choose values for name/location/etc from DnB columns
            (Req 3.B if DUNS exist but URL is empty/NULL 
                - Update the URL for all rows where it is empty; 
                - Set IS_PRIMARY_DOMAIN = Y if there are no other URLs associated with this DUNS)
            (Have verified that there is no such condition in DnB that for one DUNS, some rows have URL but some rows don't, so set IS_PRIMARY_DOMAIN = Y directly)
        2. Node joinedCWithLeDunsWithDiffDomain = Select rows from joinedCWithLeDuns with DnB_URL not NULL (LE_DUNS = DnB_DUNS not NULL, LE_URL != DnB_URL not NULL)
            Node res3 = Select all the rows from joinedCWithLeDunsWithDiffDomain
                    and choose values for name/location/etc from DnB columns
            Node res4 = Group by LE_DUNS && LE_URL on joinedCWithLeDunsWithDiffDomain and choose only 1 row for each pair of LE_DUNS && LE_URL,
                    and choose LE_URL, set IS_PRIMARY_DOMAIN = N and choose values for name/location/etc from DnB columns
            (Req 3.C if DUNS exist but URL is NOT NULL
                - Add new DUNS x URL row
                - Copy DnB location & LE_* columns from the existing DUNS row where the DUNS is the same
                - Set IS_PRIMARY_DOMAIN = N)
        3. Node res5 = Select all the rows from joinedCWithoutLeDuns
                   and choose values for name/location/etc from DnB columns
        
        D) Node joinedD = dnbRefined RIGHT JOIN le(URL) by URL
        1. Node res6 = Select rows from joinedD with DnB_URL NULL
                   and choose LE_URL, set IS_PRIMARY_DOMAIN = Y, IS_PRIMARY_LOCATION = Y, NUMBER_OF_LOCATIONS = 1
                   and choose values for name/location/etc from LE columns
            
        E) Node AccountMasterSeed = Merge res1, res2, res3, res4, res5 and res6
        
        F) Retain columns for AccountMasterSeed, remove prefix "TMP_" in column names ("TMP_" is used to avoid conflicts in previous steps), and add Timestamp column
        
        Example:
        dnb:
        DUNS    URL
        01      a.com
        01      c.com
        03      a.com
        02
        04
        
        le:
        DUNS    URL
        01      a.com
        01      b.com
        02      a.com
                c.com
                d.com
                
        joinedA:
        DnB_DUNS    DnB_URL     LE_DUNS     LE_URL
        01          a.com       01          a.com
        01          c.com
        03          a.com
        02
        04
                                01          b.com
                                02          a.com
                                            c.com
                                            d.com
                                            
        Temp result for A1:
        DnB_DUNS    DnB_URL     LE_DUNS     LE_URL
        01          a.com       01          a.com
        
        res1:
        DUNS    URL     LE_IS_PRIMARY_DOMAIN    LE_IS_PRIMARY_LOCATION      LE_NUMBER_OF_LOCATION   etc
        01      a.com   from DnB                from DnB                    from DnB                from DnB
        
        dnbRefined:
        DnB_DUNS    DnB_URL
        01          c.com
        03          a.com
        02
        04
        
        leDunsDomain
        LE_DUNS     LE_URL
        01          b.com
        02          a.com
        
        leOnlyDomain
        LE_DUNS     LE_URL
                    c.com
                    d.com
                    
        joinedC
        DnB_DUNS    DnB_URL     LE_DUNS     LE_URL
        01          c.com       01          b.com
        03          a.com
        02                      02          a.com
        04
        
        joinedCWithLeDuns
        DnB_DUNS    DnB_URL     LE_DUNS     LE_URL
        01          c.com       01          b.com
        02                      02          a.com
        
        joinedCWithoutLeDuns
        DnB_DUNS    DnB_URL     LE_DUNS     LE_URL
        03          a.com
        04
        
        res2
        DUNS    URL     LE_IS_PRIMARY_DOMAIN    LE_IS_PRIMARY_LOCATION      LE_NUMBER_OF_LOCATION   etc
        02      a.com   Y                       from DnB                    from DnB                from DnB
        
        joinedCWithLeDunsWithDiffDomain
        DnB_DUNS    DnB_URL     LE_DUNS     LE_URL
        01          c.com       01          b.com
        
        res3
        DUNS    URL     LE_IS_PRIMARY_DOMAIN    LE_IS_PRIMARY_LOCATION      LE_NUMBER_OF_LOCATION   etc
        01      c.com   from DnB                from DnB                    from DnB                from DnB
        
        res4
        DUNS    URL     LE_IS_PRIMARY_DOMAIN    LE_IS_PRIMARY_LOCATION      LE_NUMBER_OF_LOCATION   etc
        01      b.com   N                       from DnB                    from DnB                from DnB
        
        res5
        DUNS    URL     LE_IS_PRIMARY_DOMAIN    LE_IS_PRIMARY_LOCATION      LE_NUMBER_OF_LOCATION   etc
        03      a.com   from DnB                from DnB                    from DnB                from DnB
        04              from DnB                from DnB                    from DnB                from DnB
        
        joinedD
        DnB_DUNS    DnB_URL     LE_DUNS     LE_URL
        01          c.com                   c.com
                                            d.com
        
        res6
        DUNS    URL     LE_IS_PRIMARY_DOMAIN    LE_IS_PRIMARY_LOCATION      LE_NUMBER_OF_LOCATION   etc
                d.com   Y                       Y                           1                       from LE
                
        Temp AccountMasterSeed (Merge res1, res2, res3, res4, res5, res6)
        DUNS    URL     LE_IS_PRIMARY_DOMAIN    LE_IS_PRIMARY_LOCATION      LE_NUMBER_OF_LOCATION   etc
        01      a.com   from DnB                from DnB                    from DnB                from DnB
        01      c.com   from DnB                from DnB                    from DnB                from DnB
        01      b.com   N                       from DnB                    from DnB                from DnB
        02      a.com   Y                       from DnB                    from DnB                from DnB
        03      a.com   from DnB                from DnB                    from DnB                from DnB
        04              from DnB                from DnB                    from DnB                from DnB
                d.com   Y                       Y                           1                       from LE    
     */

}
