package com.latticeengines.eai.service.impl.salesforce;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.eai.Attribute;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.exposed.util.AvroSchemaBuilder;
import com.latticeengines.eai.routes.converter.AvroTypeConverter;
import com.latticeengines.eai.service.ImportService;

@Component("salesforceImportService")
public class SalesforceImportServiceImpl extends ImportService {

    @Autowired
    private ProducerTemplate producer;

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

    private JobInfo setupJob(Table table) {
        JobInfo jobInfo = new JobInfo();
        jobInfo.setOperation(OperationEnum.QUERY);
        jobInfo.setContentType(ContentType.XML);
        jobInfo.setObject(table.getName());
        jobInfo = producer.requestBody("direct:createJob", jobInfo, JobInfo.class);
        return jobInfo;
    }

    @Override
    public List<Table> importMetadata(List<Table> tables) {
        List<Table> newTables = new ArrayList<>();
        for (Table table : tables) {
            JobInfo jobInfo = setupJob(table);
            try {
                SObjectDescription desc = producer.requestBodyAndHeader("direct:getDescription", jobInfo,
                        SalesforceEndpointConfig.SOBJECT_NAME, table.getName(), SObjectDescription.class);
                List<SObjectField> descFields = desc.getFields();
                Map<String, Attribute> map = table.getNameAttributeMap();
                Table newTable = new Table();
                newTable.setName(table.getName());
                newTable.setDisplayName(desc.getLabel());

                for (SObjectField descField : descFields) {
                    if (!map.containsKey(descField.getName())) {
                        continue;
                    }
                    Attribute attr = new Attribute();
                    String type = descField.getType();
                    attr.setName(descField.getName());
                    attr.setDisplayName(descField.getLabel());
                    attr.setLength(descField.getLength());
                    attr.setPrecision(descField.getPrecision());
                    attr.setScale(descField.getScale());
                    attr.setNullable(descField.isNillable());
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
                        List<String> enumValues = Lists.transform(descField.getPicklistValues(),
                                ToStringFunction.INSTANCE);
                        attr.setCleanedUpEnumValues(cleanedUpEnumValues);
                        attr.setEnumValues(enumValues);
                    }

                    newTable.addAttribute(attr);
                }
                Schema schema = AvroSchemaBuilder.createSchema(newTable.getName(), newTable);
                newTable.setSchema(schema);
                newTables.add(newTable);
            } finally {
                producer.requestBody("salesforce:closeJob", jobInfo, JobInfo.class);
            }
        }
        return newTables;
    }

    @Override
    public void importDataAndWriteToHdfs(List<Table> tables, ImportContext context) {
        for (Table table : tables) {
            JobInfo jobInfo = setupJob(table);
            String query = createQuery(table);
            Map<String, Object> headers = new HashMap<String, Object>();
            headers.put(SalesforceEndpointConfig.SOBJECT_QUERY, query);
            super.setHeaders(headers, table, context);
            producer.sendBodyAndHeaders("direct:createBatchQuery", jobInfo, headers);
        }
    }

    String createQuery(Table table) {
        return "SELECT " + StringUtils.join(table.getAttributes(), ",") + " FROM " + table.getName();
    }

}
