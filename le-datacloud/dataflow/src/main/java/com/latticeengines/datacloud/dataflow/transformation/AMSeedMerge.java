package com.latticeengines.datacloud.dataflow.transformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMSeedMergeWithDunsBuffer;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMSeedMergeWithoutDunsFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn.Calculation;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

import cascading.tuple.Fields;

@Component(AMSeedMerge.DATAFLOW_BEAN_NAME)
public class AMSeedMerge extends ConfigurableFlowBase<TransformerConfig> {

    public static final String DATAFLOW_BEAN_NAME = "AMSeedMerge";
    public static final String TRANSFORMER_NAME = "AMSeedMergeTransformer";

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(AMSeedMerge.class);

    private Map<String, SeedMergeFieldMapping> amsColumns = new HashMap<>();
    // dnb columns -> ams columns
    private Map<String, String> dnbToAms = new HashMap<>();
    // le columns -> ams columns
    private Map<String, String> leToAms = new HashMap<>();
    // ams columns -> le columns
    private Map<String, String> amsToLe = new HashMap<>();
    // ams columns -> dnb columns
    private Map<String, String> amsToDnB = new HashMap<>();
    private List<String> amsAttrs = new ArrayList<String>();

    private String dnbDunsCol;
    private String leDunsCol;
    private String amsDunsCol;
    private String dnbDomainCol;
    private String leDomainCol;
    private String amsDomainCol;
    private String dnbIsPrimaryDomainCol;
    private String amsIsPrimaryDomainCol;
    private String amsIsPrimaryLocationCol;
    private String amsNumberOfLocationCol;
    private String amsDomainSourceCol = "DomainSource";
    private String leDomainSourceCol = "__Source__"; // Hardcode temporarily. Will migrate to pipeline config later
    private String amsEmployeesHere;
    private String amsSalesVolumeUsDollars;
    private String leSourcePriority = "__Source_Priority__";

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

        Node ams = dnb.join(new FieldList(dnbDunsCol), le, new FieldList(leDunsCol), JoinType.OUTER);

        // Process joined rows with Duns
        Node amsWithDuns = ams.filter(String.format("%s != null", dnbDunsCol), new FieldList(dnbDunsCol));
        amsWithDuns = processWithDuns(amsWithDuns);

        // Process joined rows without Duns
        Node amsWithoutDuns = ams.filter(String.format("%s == null", dnbDunsCol), new FieldList(dnbDunsCol))
                .groupByAndLimit(new FieldList(leDomainCol), new FieldList(leSourcePriority), 1, true, false);
        // Rename is needed otherwise merge operation later will fail
        Node amsWithDunsRenamed = amsWithDuns.renamePipe("AmsWithDuns").retain(new FieldList(amsDomainCol));
        amsWithoutDuns = amsWithoutDuns.join(new FieldList(leDomainCol), amsWithDunsRenamed,
                new FieldList(amsDomainCol), JoinType.LEFT);
        amsWithoutDuns = amsWithoutDuns
                .filter(String.format("%s == null", "AmsWithDuns__" + amsDomainCol),
                        new FieldList("AmsWithDuns__" + amsDomainCol))
                .discard(new FieldList("AmsWithDuns__" + amsDomainCol));
        amsWithoutDuns = processWithoutDuns(amsWithoutDuns);

        Node amsMerged = amsWithDuns.merge(amsWithoutDuns);
        
        amsMerged = addColumnNode(amsMerged, parameters.getColumns());

        return amsMerged;
    }

    private Node processWithDuns(Node source) {
        List<FieldMetadata> fieldMetadata = prepareAmsFieldMetadata();
        source = source.groupByAndBuffer(new FieldList(dnbDunsCol),
                new AMSeedMergeWithDunsBuffer(new Fields(amsAttrs.toArray(new String[amsAttrs.size()])), amsToDnB,
                        dnbDomainCol, leDomainCol, dnbIsPrimaryDomainCol, amsIsPrimaryDomainCol, amsDomainCol,
                        amsDomainSourceCol, leDomainSourceCol),
                fieldMetadata);
        return source;
    }

    private List<FieldMetadata> prepareAmsFieldMetadata() {
        List<FieldMetadata> fieldMetadata = new ArrayList<FieldMetadata>();
        for (String attr : amsAttrs) {
            if (attr.equals(amsNumberOfLocationCol) || attr.equals(amsEmployeesHere)) {
                fieldMetadata.add(new FieldMetadata(attr, Integer.class));
            } else if (attr.equals(amsSalesVolumeUsDollars)) {
                fieldMetadata.add(new FieldMetadata(attr, Long.class));
            } else {
                fieldMetadata.add(new FieldMetadata(attr, String.class));
            }
        }
        return fieldMetadata;
    }

    private String getAmsAttrTmp(String name) {
        return "AMS_" + name;
    }

    private List<String> getAmsAttrsTmp() {
        List<String> amsAttrsTmp = new ArrayList<String>();
        for (String amsAttr : amsAttrs) {
            amsAttrsTmp.add(getAmsAttrTmp(amsAttr));
        }
        return amsAttrsTmp;
    }

    private Map<String, String> getAttrsFromLeTmp() {
        Map<String, String> attrsFromLeTmp = new HashMap<>();
        for (Map.Entry<String, String> entry : amsToLe.entrySet()) {
            attrsFromLeTmp.put(getAmsAttrTmp(entry.getKey()), entry.getValue());
        }
        return attrsFromLeTmp;
    }

    private Node processWithoutDuns(Node source) {
        List<String> amsAttrsTmp = getAmsAttrsTmp();
        List<FieldMetadata> fieldMetadataTmp = prepareAmsFieldMetadataTmp(amsAttrsTmp);
        Map<String, String> attrsFromLeTmp = getAttrsFromLeTmp();
        source = source.apply(
                new AMSeedMergeWithoutDunsFunction(new Fields(amsAttrsTmp.toArray(new String[amsAttrsTmp.size()])),
                        attrsFromLeTmp, getAmsAttrTmp(amsDunsCol), getAmsAttrTmp(amsIsPrimaryDomainCol),
                        getAmsAttrTmp(amsIsPrimaryLocationCol), getAmsAttrTmp(amsNumberOfLocationCol),
                        getAmsAttrTmp(amsDomainSourceCol), leDomainSourceCol),
                new FieldList(source.getFieldNames()), fieldMetadataTmp, new FieldList(amsAttrsTmp));
        for (String amsAttr : amsAttrs) {
            source = source.rename(new FieldList(getAmsAttrTmp(amsAttr)), new FieldList(amsAttr));
        }
        return source;
    }

    private List<FieldMetadata> prepareAmsFieldMetadataTmp(List<String> amsAttrsTmp) {
        List<FieldMetadata> fieldMetadata = new ArrayList<FieldMetadata>();
        for (String attr : amsAttrsTmp) {
            if (attr.equals(getAmsAttrTmp(amsNumberOfLocationCol)) || attr.equals(getAmsAttrTmp(amsEmployeesHere))) {
                fieldMetadata.add(new FieldMetadata(attr, Integer.class));
            } else if (attr.equals(getAmsAttrTmp(amsSalesVolumeUsDollars))) {
                fieldMetadata.add(new FieldMetadata(attr, Long.class));
            } else {
                fieldMetadata.add(new FieldMetadata(attr, String.class));
            }
        }
        return fieldMetadata;
    }

    private Node retainDnBColumns(Node node) {
        List<String> columnNames = new ArrayList<>(dnbToAms.keySet());
        return node.retain(new FieldList(columnNames));
    }

    private Node retainLeColumns(Node node) {
        List<String> columnNames = new ArrayList<>(leToAms.keySet());
        columnNames.add(leDomainSourceCol);
        columnNames.add(leSourcePriority);
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

    private void getColumnMapping(List<SourceColumn> sourceColumns) throws IOException {
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
            amsToLe.put(item.getTargetColumn(), item.getLeColumn());
            amsToDnB.put(item.getTargetColumn(), item.getDnbColumn());
            amsAttrs.add(item.getTargetColumn());

            amsColumns.put(item.getTargetColumn(), item);
            dnbToAms.put(item.getDnbColumn(), item.getTargetColumn());
            if (item.getLeColumn() != null) {
                leToAms.put(item.getLeColumn(), item.getTargetColumn());
            }
            switch (item.getColumnType()) {
            case DOMAIN:
                dnbDomainCol = item.getDnbColumn();
                leDomainCol = item.getLeColumn();
                amsDomainCol = item.getTargetColumn();
                break;
            case DUNS:
                dnbDunsCol = item.getDnbColumn();
                leDunsCol = item.getLeColumn();
                amsDunsCol = item.getTargetColumn();
                break;
            case IS_PRIMARY_DOMAIN:
                dnbIsPrimaryDomainCol = item.getDnbColumn();
                amsIsPrimaryDomainCol = item.getTargetColumn();
                break;
            case IS_PRIMARY_LOCATION:
                amsIsPrimaryLocationCol = item.getTargetColumn();
                break;
            case NUMBER_OF_LOCATION:
                amsNumberOfLocationCol = item.getTargetColumn();
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

        amsAttrs.add(amsDomainSourceCol);
    }

    @Override
    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return TransformerConfig.class;
    }

    static class SeedMergeFieldMapping {
        private String targetColumn;

        private String dnbColumn;

        private String leColumn;

        private ColumnType columnType;

        @JsonProperty("TargetColumn")
        public String getTargetColumn() {
            return targetColumn;
        }

        @JsonProperty("TargetColumn")
        public void setTargetColumn(String targetColumn) {
            this.targetColumn = targetColumn;
        }

        @JsonProperty("DnBColumn")
        public String getDnbColumn() {
            return dnbColumn;
        }

        @JsonProperty("DnBColumn")
        public void setDnbColumn(String dnbColumn) {
            this.dnbColumn = dnbColumn;
        }

        @JsonProperty("LeColumn")
        public String getLeColumn() {
            return leColumn;
        }

        @JsonProperty("LeColumn")
        public void setLeColumn(String leColumn) {
            this.leColumn = leColumn;
        }

        @JsonProperty("ColumnType")
        public ColumnType getColumnType() {
            return columnType;
        }

        @JsonProperty("ColumnType")
        public void setColumnType(ColumnType columnType) {
            this.columnType = columnType;
        }
    }

    enum ColumnType {
        DOMAIN, DUNS, IS_PRIMARY_DOMAIN, IS_PRIMARY_LOCATION, NUMBER_OF_LOCATION, DU_DUNS, EMPLOYEES_HERE, SALES_VOLUME_US_DOLLARS, OTHER
    }
}
