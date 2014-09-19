package com.latticeengines.eai.service.impl.salesforce;

import java.util.ArrayList;
import java.util.List;

import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.salesforce.SalesforceEndpointConfig;
import org.apache.camel.component.salesforce.api.dto.SObjectDescription;
import org.apache.camel.component.salesforce.api.dto.bulk.ContentType;
import org.apache.camel.component.salesforce.api.dto.bulk.JobInfo;
import org.apache.camel.component.salesforce.api.dto.bulk.OperationEnum;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.service.ImportService;

@Component("salesforceImportService")
public class SalesforceImportServiceImpl implements ImportService {

    @Autowired
    private ProducerTemplate producer;

    private List<Table> tables;
    private List<JobInfo> jobInfos;

    @Override
    public void init(List<Table> tables) {
        assert (tables != null);
        this.tables = tables;
        this.jobInfos = new ArrayList<JobInfo>(tables.size());
        for (Table table : tables) {
            JobInfo jobInfo = new JobInfo();
            jobInfo.setOperation(OperationEnum.QUERY);
            jobInfo.setContentType(ContentType.XML);
            jobInfo.setObject(table.getName());
            jobInfo = producer.requestBody("direct:createJob", jobInfo, JobInfo.class);
            jobInfos.add(jobInfo);
        }
    }

    @Override
    public void importMetadata() {
        for (JobInfo jobInfo : jobInfos) {
            SObjectDescription desc = producer.requestBodyAndHeader("direct:getDescription", jobInfo,
                    SalesforceEndpointConfig.SOBJECT_NAME, jobInfo.getObject(), SObjectDescription.class);
            System.out.println(desc);
        }
    }

    @Override
    public void finalize() {
        for (JobInfo jobInfo : jobInfos) {
            producer.requestBody("salesforce:closeJob", jobInfo, JobInfo.class);
        }
    }

    @Override
    public void importData() {
        for (int i = 0; i < jobInfos.size(); i++) {
            String query = createQuery(tables.get(i));
            producer.sendBodyAndHeader("direct:createBatchQuery", jobInfos.get(i),
                    SalesforceEndpointConfig.SOBJECT_QUERY, query);
        }
    }

    String createQuery(Table table) {
        return "SELECT " + StringUtils.join(table.getAttributes(), ",") + " FROM " + table.getName();
    }

}
