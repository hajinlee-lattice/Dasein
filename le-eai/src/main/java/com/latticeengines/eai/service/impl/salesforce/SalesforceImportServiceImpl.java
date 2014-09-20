package com.latticeengines.eai.service.impl.salesforce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.salesforce.SalesforceEndpointConfig;
import org.apache.camel.component.salesforce.api.dto.SObjectDescription;
import org.apache.camel.component.salesforce.api.dto.SObjectField;
import org.apache.camel.component.salesforce.api.dto.bulk.ContentType;
import org.apache.camel.component.salesforce.api.dto.bulk.JobInfo;
import org.apache.camel.component.salesforce.api.dto.bulk.OperationEnum;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.Attribute;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.routes.salesforce.SalesforceImportProperty;
import com.latticeengines.eai.service.ImportService;

@Component("salesforceImportService")
public class SalesforceImportServiceImpl implements ImportService {

    @Autowired
    private ProducerTemplate producer;

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
                for (SObjectField descField : descFields) {
                    if (!map.containsKey(descField.getName())) {
                        continue;
                    }
                    Attribute attr = new Attribute();
                    attr.setName(descField.getName());
                    attr.setDisplayName(descField.getLabel());
                    attr.setLength(descField.getLength());
                    attr.setPrecision(descField.getPrecision());
                    attr.setScale(descField.getScale());
                    attr.setNullable(descField.isNillable());
                    attr.setPhysicalDataType(descField.getType());
                    attr.setLogicalDataType(descField.getType());
                    
                    newTable.addAttribute(attr);
                }
                newTables.add(newTable);
            } finally {
                producer.requestBody("salesforce:closeJob", jobInfo, JobInfo.class);
            }
        }
        return newTables;
    }
    
    @Override
    public void importData(List<Table> tables) {
        for (Table table : tables) {
            JobInfo jobInfo = setupJob(table);
            String query = createQuery(table);
            Map<String, Object> headers = new HashMap<String, Object>();
            headers.put(SalesforceEndpointConfig.SOBJECT_QUERY, query);
            headers.put(SalesforceImportProperty.TABLE, table);
            producer.sendBodyAndHeaders("direct:createBatchQuery", jobInfo, headers);
        }
    }

    String createQuery(Table table) {
        return "SELECT " + StringUtils.join(table.getAttributes(), ",") + " FROM " + table.getName();
    }

}
