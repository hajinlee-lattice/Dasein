package com.latticeengines.datacloud.dataflow.transformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterSeedMergeWithDunsBuffer;
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterSeedMergeWithoutDunsFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn.Calculation;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

import cascading.tuple.Fields;

@Component("accountMasterSeedMergeFlow")
public class AccountMasterSeedMergeFlow extends ConfigurableFlowBase<TransformerConfig> {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(AccountMasterSeedMergeFlow.class);

    private Map<String, SeedMergeFieldMapping> accountMasterIntermediateSeedColumnMapping = new HashMap<String, SeedMergeFieldMapping>();
    // dnbCacheSeed columns -> accountMasterIntermediateSeed columns
    private Map<String, String> dnbCacheSeedColumnMapping = new HashMap<String, String>();
    // latticeCacheSeed columns -> accountMasterIntermediateSeed columns
    private Map<String, String> latticeCacheSeedColumnMapping = new HashMap<String, String>();

    // ams columns -> le columns
    private Map<String, String> attrsFromLe = new HashMap<>();
    // ams columns -> dnb columns
    private Map<String, String> attrsFromDnB = new HashMap<>();
    private List<String> amsAttrs = new ArrayList<String>();

    private String dnbDunsColumn;
    private String leDunsColumn;
    private String amsDunsColumn;
    private String dnbDomainColumn;
    private String leDomainColumn;
    private String amsDomainColumn;
    private String dnbIsPrimaryDomainColumn;
    private String amsIsPrimaryDomainColumn;
    private String amsIsPrimaryLocationColumn;
    private String amsNumberOfLocationColumn;
    private String dnbDuDunsColumn;
    private String amsDomainSourceColumn = "DomainSource";
    private String amsEmployeesHere;
    private String amsSalesVolumeUsDollars;

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        try {
            getColumnMapping(parameters.getColumns());
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_25010, e);
        }
        Node dnb = addSource(parameters.getBaseTables().get(0));
        dnb = retainDnBColumns(dnb);
        Node le = addSource(parameters.getBaseTables().get(1));
        le = retainLeColumns(le);

        Node ams = dnb.join(new FieldList(dnbDunsColumn), le, new FieldList(leDunsColumn), JoinType.OUTER);
        Node amsWithDuns = ams.filter(String.format("%s != null", dnbDunsColumn), new FieldList(dnbDunsColumn));
        Node amsWithoutDuns = ams.filter(String.format("%s == null", dnbDunsColumn), new FieldList(dnbDunsColumn));

        amsWithDuns = processWithDuns(amsWithDuns);
        amsWithoutDuns = processWithoutDuns(amsWithoutDuns);

        Node amsMerged = amsWithDuns.merge(amsWithoutDuns);
        amsMerged = addColumnNode(amsMerged, parameters.getColumns());

        return amsMerged;
    }

    private Node processWithDuns(Node source) {
        List<FieldMetadata> fieldMetadata = prepareAmsFieldMetadata();
        source = source.groupByAndBuffer(new FieldList(dnbDunsColumn),
                new AccountMasterSeedMergeWithDunsBuffer(new Fields(amsAttrs.toArray(new String[amsAttrs.size()])),
                        attrsFromDnB, dnbDunsColumn, dnbDomainColumn, leDomainColumn, dnbIsPrimaryDomainColumn,
                        dnbDuDunsColumn, amsIsPrimaryDomainColumn, amsDomainColumn, amsDomainSourceColumn),
                fieldMetadata);
        return source;
    }

    private Node processWithoutDuns(Node source) {
        List<FieldMetadata> fieldMetadata = prepareAmsFieldMetadata();
        source = source.apply(
                new AccountMasterSeedMergeWithoutDunsFunction(new Fields(amsAttrs.toArray(new String[amsAttrs.size()])),
                        attrsFromLe, amsDunsColumn, amsIsPrimaryDomainColumn, amsIsPrimaryLocationColumn,
                        amsNumberOfLocationColumn, amsDomainSourceColumn),
                new FieldList(source.getFieldNames()), fieldMetadata, new FieldList(amsAttrs), Fields.RESULTS);
        return source;
    }

    private List<FieldMetadata> prepareAmsFieldMetadata() {
        List<FieldMetadata> fieldMetadata = new ArrayList<FieldMetadata>();
        for (String attr : amsAttrs) {
            if (attr.equals(amsNumberOfLocationColumn) || attr.equals(amsEmployeesHere)) {
                fieldMetadata.add(new FieldMetadata(attr, Integer.class));
            } else if (attr.equals(amsSalesVolumeUsDollars)) {
                fieldMetadata.add(new FieldMetadata(attr, Long.class));
            } else {
                fieldMetadata.add(new FieldMetadata(attr, String.class));
            }
        }
        return fieldMetadata;
    }

    private Node retainDnBColumns(Node node) {
        List<String> columnNames = new ArrayList<String>(dnbCacheSeedColumnMapping.keySet());
        return node.retain(new FieldList(columnNames));
    }

    private Node retainLeColumns(Node node) {
        List<String> columnNames = new ArrayList<String>(latticeCacheSeedColumnMapping.keySet());
        return node.retain(new FieldList(columnNames));
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
            attrsFromLe.put(item.getTargetColumn(), item.getLeColumn());
            attrsFromDnB.put(item.getTargetColumn(), item.getDnbColumn());
            amsAttrs.add(item.getTargetColumn());

            accountMasterIntermediateSeedColumnMapping.put(item.getTargetColumn(), item);
            dnbCacheSeedColumnMapping.put(item.getDnbColumn(), item.getTargetColumn());
            if (item.getLeColumn() != null) {
                latticeCacheSeedColumnMapping.put(item.getLeColumn(), item.getTargetColumn());
            }
            switch (item.getColumnType()) {
            case DOMAIN:
                dnbDomainColumn = item.getDnbColumn();
                leDomainColumn = item.getLeColumn();
                amsDomainColumn = item.getTargetColumn();
                break;
            case DUNS:
                dnbDunsColumn = item.getDnbColumn();
                leDunsColumn = item.getLeColumn();
                amsDunsColumn = item.getTargetColumn();
                break;
            case IS_PRIMARY_DOMAIN:
                dnbIsPrimaryDomainColumn = item.getDnbColumn();
                amsIsPrimaryDomainColumn = item.getTargetColumn();
                break;
            case IS_PRIMARY_LOCATION:
                amsIsPrimaryLocationColumn = item.getTargetColumn();
                break;
            case NUMBER_OF_LOCATION:
                amsNumberOfLocationColumn = item.getTargetColumn();
                break;
            case DU_DUNS:
                dnbDuDunsColumn = item.getDnbColumn();
                break;
            case EMPLOYEES_HERE:
                amsEmployeesHere = item.getTargetColumn();
                break;
            case SALES_VOLUME_US_DOLLARS:
                amsSalesVolumeUsDollars = item.getTargetColumn();
                break;
            default:
                break;
            }
        }

        amsAttrs.add(amsDomainSourceColumn);
    }

    @Override
    public String getDataFlowBeanName() {
        return "accountMasterSeedMergeFlow";
    }

    @Override
    public String getTransformerName() {
        return "accountMasterSeedMergeTransformer";
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return TransformerConfig.class;
    }

}