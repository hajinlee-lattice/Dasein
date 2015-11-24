package com.latticeengines.eai.service.impl.salesforce.strategy;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.salesforce.SalesforceEndpointConfig;
import org.apache.camel.component.salesforce.api.dto.PickListValue;
import org.apache.camel.component.salesforce.api.dto.SObjectDescription;
import org.apache.camel.component.salesforce.api.dto.SObjectField;
import org.apache.camel.component.salesforce.api.dto.bulk.ContentType;
import org.apache.camel.component.salesforce.api.dto.bulk.JobInfo;
import org.apache.camel.component.salesforce.api.dto.bulk.OperationEnum;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.eai.exposed.util.AvroSchemaBuilder;
import com.latticeengines.eai.metadata.util.EaiMetadataUtil;
import com.latticeengines.eai.service.impl.AvroTypeConverter;
import com.latticeengines.eai.service.impl.ImportStrategy;

@Component
public class SalesforceImportStrategyBase extends ImportStrategy {
    private static final Log log = LogFactory.getLog(SalesforceImportStrategyBase.class);

    @Autowired
    private AvroTypeConverter salesforceToAvroTypeConverter;

    private enum ToStringFunctionWithCleanup implements Function<PickListValue, String> {
        INSTANCE;

        @Override
        public String toString() {
            return "toString";
        }

        @Override
        public String apply(PickListValue input) {
            checkNotNull(input);
            return AvroUtils.getAvroFriendlyString(input.getValue());
        }
    }

    private enum ToStringFunction implements Function<PickListValue, String> {
        INSTANCE;

        @Override
        public String toString() {
            return "toString";
        }

        @Override
        public String apply(PickListValue input) {
            checkNotNull(input);
            return input.getValue();
        }
    }

    public SalesforceImportStrategyBase() {
        this("Salesforce.AllTables");
    }

    public SalesforceImportStrategyBase(String key) {
        super(key);
    }

    private JobInfo setupJob(ProducerTemplate template, Table table) {
        JobInfo jobInfo = new JobInfo();
        jobInfo.setOperation(OperationEnum.QUERY);
        jobInfo.setContentType(ContentType.XML);
        jobInfo.setObject(table.getName());
        jobInfo = template.requestBody("direct:createJob", jobInfo, JobInfo.class);
        return jobInfo;
    }

    /**
     * Invoke this method when about to invoke a Camel route to do the import.
     * This method will set the required headers for the framework.
     * 
     * @param headers
     *            map that will hold the headers
     * @param table
     *            table that will be passed into the import route
     * @param context
     *            import context
     */
    private void setHeaders(Map<String, Object> headers, Table table, ImportContext context) {
        if (headers != null) {
            headers.put(ImportProperty.TABLE, table);
            headers.put(ImportProperty.IMPORTCTX, context);
        } else {
            log.warn("headers should not be null. No headers have been set.");
        }
    }

    @Override
    public void importData(ProducerTemplate template, Table table, String filter, ImportContext ctx) {
        JobInfo jobInfo = setupJob(template, table);
        String query = createQuery(table, filter);
        Map<String, Object> headers = new HashMap<String, Object>();
        headers.put(SalesforceEndpointConfig.SOBJECT_QUERY, query);
        setHeaders(headers, table, ctx);
        template.sendBodyAndHeaders("direct:createBatchQuery", jobInfo, headers);
    }

    @Override
    public Table importMetadata(ProducerTemplate template, Table table, String filter, ImportContext ctx) {
        JobInfo jobInfo = setupJob(template, table);

        PrimaryKey pk = EaiMetadataUtil.createPrimaryKey();

        try {
            SObjectDescription desc = template.requestBodyAndHeader("direct:getDescription", jobInfo,
                    SalesforceEndpointConfig.SOBJECT_NAME, table.getName(), SObjectDescription.class);
            List<SObjectField> descFields = desc.getFields();
            Map<String, Attribute> nameAttrMap = table.getNameAttributeMap();
            validateSalesforceMetadata(table.getName(), nameAttrMap, descFields);

            Table newTable = new Table();
            newTable.setName(table.getName());
            newTable.setDisplayName(desc.getLabel());

            for (SObjectField descField : descFields) {
                if (!nameAttrMap.containsKey(descField.getName())) {
                    continue;
                }
                Attribute attrFromImportTables = nameAttrMap.get(descField.getName());
                Attribute attr = new Attribute();
                String type = descField.getType();

                attr.setName(descField.getName());
                attr.setName(descField.getName());
                attr.setDisplayName(descField.getLabel());
                attr.setLength(descField.getLength());
                attr.setPrecision(descField.getPrecision());
                attr.setScale(descField.getScale());
                attr.setNullable(descField.isNillable());

                attr.setApprovedUsage(attrFromImportTables.getApprovedUsage());
                attr.setDataSource(attrFromImportTables.getDataSource());
                attr.setDataQuality(attrFromImportTables.getDataQuality());
                attr.setDescription(attrFromImportTables.getDescription());
                attr.setDisplayDiscretizationStrategy(attrFromImportTables.getDisplayDiscretizationStrategy());
                attr.setCategory(attrFromImportTables.getCategory());
                attr.setDataType(attrFromImportTables.getDataType());
                attr.setFundamentalType(attrFromImportTables.getFundamentalType());
                attr.setPhysicalName(attr.getName());
                attr.setSemanticType(attrFromImportTables.getSemanticType());
                if (StringUtils.isNotEmpty(attr.getSemanticType())) {
                    attr.setName(attr.getSemanticType());
                }
                attr.setStatisticalType(attrFromImportTables.getStatisticalType());
                attr.setTags(Arrays.asList(new String[] { ModelingMetadata.INTERNAL_TAG }));

                Schema.Type avroType = salesforceToAvroTypeConverter.convertTypeToAvro(type);

                if (avroType == null) {
                    throw new RuntimeException("Could not find avro type for sfdc type " + type);
                }
                attr.setPhysicalDataType(avroType.name());
                attr.setLogicalDataType(type);

                if (type.equals("picklist")) {
                    List<PickListValue> values = descField.getPicklistValues();
                    PickListValue emptyValue = new PickListValue();
                    emptyValue.setValue(" ");
                    values.add(emptyValue);
                    List<String> cleanedUpEnumValues = Lists.transform(descField.getPicklistValues(),
                            ToStringFunctionWithCleanup.INSTANCE);
                    List<String> enumValues = Lists.transform(descField.getPicklistValues(), ToStringFunction.INSTANCE);
                    attr.setCleanedUpEnumValues(cleanedUpEnumValues);
                    attr.setEnumValues(enumValues);
                } else if (type.equals("date")) {
                    attr.setPropertyValue("dateFormat", "YYYY-MM-DD");
                } else if (type.equals("datetime")) {
                    attr.setPropertyValue("dateFormat", "YYYY-MM-DD'T'HH:mm:ss.sssZ");
                }
                if (attr.getLogicalDataType().equals("id")) {
                    pk.addAttribute(attr.getName());
                }
                newTable.addAttribute(attr);
            }
            newTable.setPrimaryKey(table.getPrimaryKey());
            newTable.setLastModifiedKey(table.getLastModifiedKey());

            Schema schema = AvroSchemaBuilder.createSchema(newTable.getName(), newTable);
            newTable.setSchema(schema);
            return newTable;
        } finally {
            template.requestBody("salesforce:closeJob", jobInfo, JobInfo.class);
        }
    }

    protected void validateSalesforceMetadata(String table, Map<String, Attribute> nameAttrMap,
            List<SObjectField> descFields) {
        List<String> descFieldNames = new ArrayList<>();
        for (SObjectField descField : descFields) {
            descFieldNames.add(descField.getName());
        }
        validateMetadata(table, nameAttrMap, descFieldNames);
    }

    @Override
    public ImportContext resolveFilterExpression(String expression, ImportContext ctx) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected AvroTypeConverter getAvroTypeConverter() {
        return salesforceToAvroTypeConverter;
    }

    String createQuery(Table table, String filterExpression) {
        List<Attribute> attrs = table.getAttributes();
        List<String> attrName = new ArrayList<>();
        for (Attribute attr : attrs) {
            attrName.add(attr.getPhysicalName());
        }
        String query = "SELECT " + StringUtils.join(attrName, ",") + " FROM " + table.getName();

        System.out.println(query);
        if (filterExpression != null) {
            query += " WHERE " + filterExpression;
        }
        return query;
    }

}
