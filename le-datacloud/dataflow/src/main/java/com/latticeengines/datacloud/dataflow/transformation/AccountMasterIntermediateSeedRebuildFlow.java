package com.latticeengines.datacloud.dataflow.transformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterIntermediateSeedFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.CountryStandardizationFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterIntermediateSeedParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn.Calculation;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("accountMasterIntermediateSeedRebuildFlow")
public class AccountMasterIntermediateSeedRebuildFlow
        extends TypesafeDataFlowBuilder<AccountMasterIntermediateSeedParameters> {

    private Map<String, SeedMergeFieldMapping> accountMasterIntermediateSeedColumnMapping = new HashMap<String, SeedMergeFieldMapping>();
    // dnbCacheSeed columns -> accountMasterIntermediateSeed columns
    private Map<String, String> dnbCacheSeedColumnMapping = new HashMap<String, String>();
    // latticeCacheSeed columns -> accountMasterIntermediateSeed columns
    private Map<String, String> latticeCacheSeedColumnMapping = new HashMap<String, String>();
    private String dnbDunsColumn;
    private String leDunsColumn;
    private String dnbDomainColumn;
    private String leDomainColumn;
    private String dnbIsPrimaryDomainColumn;
    private String dnbIsPrimaryLocationColumn;
    private String dnbNumberOfLocationColumn;

    private static final String COUNTRY = "Country";

    /*
     * The detailed description of implementation of PD-1196 to build
     * AccountMasterIntermediateSeed is at the bottom of this java file
     */

    @Override
    public Node construct(AccountMasterIntermediateSeedParameters parameters) {
        try {
            getColumnMapping(parameters.getColumns());
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_25010, e);
        }
        Node dnb = addSource(parameters.getBaseTables().get(0));
        dnb = retainDnBColumns(dnb);
        Node le = addSource(parameters.getBaseTables().get(1));
        le = retainLeColumns(le);

        Node dnbDunsDomain = dnb.filter(dnbDunsColumn + " != null && " + dnbDomainColumn + " != null",
                new FieldList(dnbDunsColumn, dnbDomainColumn));
        Node leOnlyDomain = le.filter(leDunsColumn + " == null", new FieldList(leDunsColumn));
        Node leDunsDomain = le.filter(leDunsColumn + " != null && " + leDomainColumn + " != null",
                new FieldList(leDunsColumn, leDomainColumn));

        // Step A
        List<String> dnbJoinFields = new ArrayList<String>();
        dnbJoinFields.add(dnbDunsColumn);
        List<String> leJoinFields = new ArrayList<String>();
        leJoinFields.add(leDunsColumn);
        Node resA = join(dnbDunsDomain, dnbJoinFields, le.limit(0), leJoinFields, JoinType.LEFT);
        resA = processA(resA);

        // Step B
        dnbJoinFields = new ArrayList<String>();
        dnbJoinFields.add(dnbDomainColumn);
        leJoinFields = new ArrayList<String>();
        leJoinFields.add(leDomainColumn);
        Node resB = join(dnbDunsDomain, dnbJoinFields, leOnlyDomain, leJoinFields, JoinType.RIGHT);
        resB = processB(resB);

        // Step C
        dnbJoinFields = new ArrayList<String>();
        dnbJoinFields.add(dnbDunsColumn);
        dnbJoinFields.add(dnbDomainColumn);
        leJoinFields = new ArrayList<String>();
        leJoinFields.add(leDunsColumn);
        leJoinFields.add(leDomainColumn);
        Node leDunsDomainRefined = join(dnb, dnbJoinFields, leDunsDomain, leJoinFields, JoinType.RIGHT);
        leDunsDomainRefined = leDunsDomainRefined.filter(dnbDunsColumn + " == null", new FieldList(dnbDunsColumn));
        leDunsDomainRefined = retainLeColumns(leDunsDomainRefined);
        Node dnbRefined = dnb.groupByAndLimit(new FieldList(dnbDunsColumn), new FieldList(dnbDomainColumn), 1, true,
                true);
        dnbJoinFields = new ArrayList<String>();
        dnbJoinFields.add(dnbDunsColumn);
        leJoinFields = new ArrayList<String>();
        leJoinFields.add(leDunsColumn);
        Node joinedC = join(dnbRefined, dnbJoinFields, leDunsDomainRefined, leJoinFields, JoinType.LEFT);
        Node resC1 = processC1(joinedC);
        Node resC2 = processC2(joinedC);
        Node resC3 = processC3(joinedC);

        Node accountMasterIntermediateSeed = resA.merge(resB).merge(resC1).merge(resC2).merge(resC3);
        accountMasterIntermediateSeed = retainAccountMasterIntermediateSeedColumnNode(accountMasterIntermediateSeed);
        accountMasterIntermediateSeed = renameAccountMasterIntermediateSeedColumnNode(accountMasterIntermediateSeed);
        accountMasterIntermediateSeed = addColumnNode(accountMasterIntermediateSeed, parameters.getColumns());
        accountMasterIntermediateSeed = accountMasterIntermediateSeed.apply(
                new CountryStandardizationFunction(COUNTRY, parameters.getStandardCountries()), new FieldList(COUNTRY),
                new FieldMetadata(COUNTRY, String.class));
        return accountMasterIntermediateSeed;
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
        node = callAccountMasterIntermediateSeedFunction(node, true, new HashSet<String>(),
                new HashMap<String, Object>());
        return node;
    }

    private Node processB(Node node) {
        String filterExpression = dnbDunsColumn + " == null";
        node = node.filter(filterExpression, new FieldList(dnbDunsColumn));
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(dnbIsPrimaryDomainColumn, "Y");
        map.put(dnbIsPrimaryLocationColumn, "Y");
        map.put(dnbNumberOfLocationColumn, 1);
        node = callAccountMasterIntermediateSeedFunction(node, false, new HashSet<String>(), map);
        return node;
    }

    private Node processC1(Node node) {
        String filterExpression = leDunsColumn + " != null && " + dnbDomainColumn + " != null";
        node = node.filter(filterExpression, new FieldList(leDunsColumn, dnbDomainColumn));
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(dnbIsPrimaryDomainColumn, "N");
        Set<String> set = new HashSet<String>();
        set.add(leDomainColumn);
        node = callAccountMasterIntermediateSeedFunction(node, true, set, map);
        return node;
    }

    private Node processC2(Node node) {
        String filterExpression = leDunsColumn + " != null && " + dnbDomainColumn + " == null";
        node = node.filter(filterExpression, new FieldList(leDunsColumn, dnbDomainColumn));
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(dnbIsPrimaryDomainColumn, "Y");
        Set<String> set = new HashSet<String>();
        set.add(leDomainColumn);
        node = callAccountMasterIntermediateSeedFunction(node, true, set, map);
        return node;
    }

    private Node processC3(Node node) {
        String filterExpression = leDunsColumn + " == null && " + dnbDomainColumn + " == null";
        node = node.filter(filterExpression, new FieldList(leDunsColumn, dnbDomainColumn));
        node = callAccountMasterIntermediateSeedFunction(node, true, new HashSet<String>(),
                new HashMap<String, Object>());
        return node;
    }

    private Node callAccountMasterIntermediateSeedFunction(Node node, boolean takeAllFromDnB, Set<String> exceptColumns,
            Map<String, Object> setDnBColumnValues) {
        for (Map.Entry<String, SeedMergeFieldMapping> entry : accountMasterIntermediateSeedColumnMapping.entrySet()) {
            String outputColumn = "Tmp_" + entry.getKey();
            String latticeColumn = entry.getValue().getLeColumn();
            String dnbColumn = entry.getValue().getDnbColumn();
            if ((takeAllFromDnB && !exceptColumns.contains(latticeColumn))
                    || (!takeAllFromDnB && exceptColumns.contains(dnbColumn))) {
                node = node.apply(
                        new AccountMasterIntermediateSeedFunction(outputColumn, dnbColumn, latticeColumn, true,
                                setDnBColumnValues),
                        new FieldList(node.getFieldNames()), new FieldMetadata(outputColumn,
                                entry.getKey().equals(dnbNumberOfLocationColumn) ? Integer.class : String.class));
            } else {
                node = node.apply(
                        new AccountMasterIntermediateSeedFunction(outputColumn, dnbColumn, latticeColumn, false,
                                setDnBColumnValues),
                        new FieldList(node.getFieldNames()), new FieldMetadata(outputColumn,
                                entry.getKey().equals(dnbNumberOfLocationColumn) ? Integer.class : String.class));
            }
        }
        return node;
    }

    private Node retainAccountMasterIntermediateSeedColumnNode(Node node) {
        List<String> columnNames = new ArrayList<String>();
        for (Map.Entry<String, SeedMergeFieldMapping> entry : accountMasterIntermediateSeedColumnMapping.entrySet()) {
            columnNames.add("Tmp_" + entry.getKey());
        }
        return node.retain(new FieldList(columnNames));
    }

    private Node renameAccountMasterIntermediateSeedColumnNode(Node node) {
        List<String> newColumnNames = new ArrayList<String>();
        List<String> oldColumnNames = new ArrayList<String>();
        for (Map.Entry<String, SeedMergeFieldMapping> entry : accountMasterIntermediateSeedColumnMapping.entrySet()) {
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
                list = om.readValue(sourceColumn.getArguments(),
                        TypeFactory.defaultInstance().constructCollectionType(List.class, SeedMergeFieldMapping.class));
                break;
            }
        }

        for (SeedMergeFieldMapping item : list) {
            accountMasterIntermediateSeedColumnMapping.put(item.getTargetColumn(), item);
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