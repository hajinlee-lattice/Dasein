package com.latticeengines.eai.exposed.service.impl;

import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.salesforce.api.dto.bulk.ContentType;
import org.apache.camel.component.salesforce.api.dto.bulk.JobInfo;
import org.apache.camel.component.salesforce.api.dto.bulk.OperationEnum;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.eai.exposed.service.DataExtractionService;

@Component("dataExtractionService")
public class DataExtractionServiceImpl implements DataExtractionService {

    @Autowired
    private ProducerTemplate producer;
    
    private JobInfo createJob(JobInfo jobInfo) throws InterruptedException {
        jobInfo = producer.requestBody("direct:createJob", jobInfo, JobInfo.class);
        return jobInfo;
    }

    @Override
    public void importData() {
        JobInfo jobInfo = new JobInfo();
        jobInfo.setOperation(OperationEnum.QUERY);
        jobInfo.setContentType(ContentType.XML);
        jobInfo.setObject("Lead");
        try {
            jobInfo = createJob(jobInfo);
            producer.sendBody("direct:createBatchQuery", jobInfo);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            producer.requestBody("salesforce:closeJob", jobInfo, JobInfo.class);
        }
        

    }

    
}
