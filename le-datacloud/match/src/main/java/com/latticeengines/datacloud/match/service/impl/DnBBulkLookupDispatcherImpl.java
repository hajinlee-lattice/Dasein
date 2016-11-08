package com.latticeengines.datacloud.match.service.impl;

import static org.springframework.http.HttpStatus.OK;

import java.util.Date;
import java.util.Map;
import java.util.UUID;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.common.exposed.util.Base64Utils;
import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.dnb.DnBBulkMatchInfo;
import com.latticeengines.datacloud.match.dnb.DnBKeyType;
import com.latticeengines.datacloud.match.dnb.DnBReturnCode;
import com.latticeengines.datacloud.match.exposed.service.DnBAuthenticationService;
import com.latticeengines.datacloud.match.exposed.service.DnBBulkLookupDispatcher;
import com.latticeengines.domain.exposed.datacloud.manage.DateTimeUtils;
import com.latticeengines.domain.exposed.datacloud.match.dnbbulkrequest.BatProcessBatchRequest;
import com.latticeengines.domain.exposed.datacloud.match.dnbbulkrequest.ObjectFactory;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

@Component
public class DnBBulkLookupDispatcherImpl extends BaseDnBLookupServiceImpl<Map<String, MatchKeyTuple>>
        implements DnBBulkLookupDispatcher {

    private static final Log log = LogFactory.getLog(DnBBulkLookupDispatcherImpl.class);

    private static final String DNB_BULK_MATCH_INFO = "DNB_BULK_MATCH_INFO";

    @Autowired
    private DnBAuthenticationService dnBAuthenticationService;

    @Value("${datacloud.dnb.bulk.url}")
    private String url;

    @Value("${datacloud.dnb.authorization.header}")
    private String authorizationHeader;

    @Value("${datacloud.dnb.application.id.header}")
    private String applicationIdHeader;

    @Value("${datacloud.dnb.application.id}")
    private String applicationId;

    @Value("${datacloud.dnb.realtime.retry.maxattempts}")
    private int retries;

    @Value("${datacloud.dnb.bulk.batch.content.object}")
    private String batchContentObject;

    @Value("${datacloud.dnb.bulk.servicebatchid.xpath}")
    private String serviceIdXpath;

    @Value("${datacloud.dnb.bulk.applicationid.xpath}")
    private String applicationIdXpath;

    @Value("${datacloud.dnb.bulk.timestamp.xpath}")
    private String timestampXpath;

    private RestTemplate restTemplate = new RestTemplate();

    @Override
    public DnBBulkMatchInfo sendRequest(Map<String, MatchKeyTuple> input) {
        DataFlowContext context = null;
        DnBBulkMatchInfo info = new DnBBulkMatchInfo();
        for (int i = 0; i < retries; i++) {
            context = executeLookup(input, DnBKeyType.bulkmatch);
            if (context.getProperty(DNB_RETURN_CODE, DnBReturnCode.class) != DnBReturnCode.EXPIRED) {
                info = context.getProperty(DNB_BULK_MATCH_INFO, DnBBulkMatchInfo.class);
                break;
            }
            dnBAuthenticationService.refreshAndGetToken(DnBKeyType.bulkmatch);
        }

        info.setDnbCode(context.getProperty(DNB_RETURN_CODE, DnBReturnCode.class));

        return info;
    }

    @Override
    protected HttpEntity<String> constructEntity(Map<String, MatchKeyTuple> input, String token) {
        String body = constructBulkRequestBody(input);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_XML);
        headers.add(applicationIdHeader, applicationId);
        headers.add(authorizationHeader, token);

        HttpEntity<String> requestEntity = new HttpEntity<>(body, headers);
        return requestEntity;
    }

    @Override
    protected void parseSucceededResponse(ResponseEntity<String> response, DataFlowContext context) {
        if (response.getStatusCode() != OK) {
            context.setProperty(DNB_RETURN_CODE, DnBReturnCode.UNKNOWN);
            return;
        }
        DnBBulkMatchInfo info = new DnBBulkMatchInfo();
        String body = response.getBody();
        info.setServiceBatchId((String) retrieveXmlValueFromResponse(serviceIdXpath, body));
        info.setApplicationId((String) retrieveXmlValueFromResponse(applicationIdXpath, body));
        info.setTimestamp((String) retrieveXmlValueFromResponse(timestampXpath, body));
        info.setDnbCode(DnBReturnCode.OK);

        context.setProperty(DNB_BULK_MATCH_INFO, info);
        context.setProperty(DNB_RETURN_CODE, info.getDnbCode());
    }

    @Override
    protected ResponseEntity<String> sendRequestToDnB(String url, HttpEntity<String> entity) {
        ResponseEntity<String> res = restTemplate.postForEntity(url, entity, String.class);
        return res;
    }

    @Override
    protected String constructUrl(Map<String, MatchKeyTuple> input) {
        return url;
    }

    private String constructBulkRequestBody(Map<String, MatchKeyTuple> input) {
        DnBBulkRequest bulkRequest = new DnBBulkRequest();
        String createdDateUTC = DateTimeUtils.formatTZ(new Date());
        String applicationID = generateApplicationID();

        String tupleStr = convertTuplesToString(input);

        bulkRequest.setBatchDetail(applicationID, createdDateUTC.toString());

        bulkRequest.setBatchSpecificationObject(batchContentObject);
        String inputObjectBase64 = Base64Utils.encodeBase64(tupleStr, false, Integer.MAX_VALUE);

        bulkRequest.setInputObjectDetail(inputObjectBase64, input.size());

        // Need to remove the first line
        String output = bulkRequest.getRequestBody();
        output = output.substring(output.indexOf("<", 2), output.length());

        return output;
    }

    private String convertTuplesToString(Map<String, MatchKeyTuple> tuples) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, MatchKeyTuple> entry : tuples.entrySet()) {
            String recordStr = constructOneRecord(entry.getKey(), entry.getValue());
            sb.append(recordStr);
            sb.append("\n");
        }
        return sb.toString();
    }

    private String constructOneRecord(String transactionId, MatchKeyTuple tuple) {
        StringBuilder record = new StringBuilder();
        record.append(",");
        record.append(transactionId);
        record.append(",,,,,,,,");
        record.append(normalizeString(tuple.getName()));
        record.append(",,,,");
        record.append(normalizeString(tuple.getCity()));
        record.append(",");
        record.append(normalizeString(tuple.getState()));
        record.append(",,,");
        record.append(normalizeString(tuple.getCountry()));
        record.append(",,,,");
        return record.toString();
    }

    private String generateApplicationID() {
        return UUID.randomUUID().toString();
    }

    private static class DnBBulkRequest {
        private static BatProcessBatchRequest request;
        private static BatProcessBatchRequest.BatchDetail batchDetail;
        private static BatProcessBatchRequest.BatchSpecification batchSpecification;
        private static BatProcessBatchRequest.BatchSpecification.BatchSpecificationObject batchSpecificationObject;
        private static BatProcessBatchRequest.InputDetail inputDetail;
        private static BatProcessBatchRequest.InputDetail.InputObjectDetail inputObjectDetail;
        private static BatProcessBatchRequest.OutputSpecification outputSpecification;

        static {
            request = (new ObjectFactory()).createBatProcessBatchRequest();
            request.setXmlnsbat("http://services.dnb.com/BatchServiceV1.0");
            request.setServiceVersionNumber(3);

            batchDetail = (new ObjectFactory()).createBatProcessBatchRequestBatchDetail();

            batchSpecification = (new ObjectFactory()).createBatProcessBatchRequestBatchSpecification();
            batchSpecificationObject = (new ObjectFactory())
                    .createBatProcessBatchRequestBatchSpecificationBatchSpecificationObject();

            inputDetail = (new ObjectFactory()).createBatProcessBatchRequestInputDetail();
            inputObjectDetail = (new ObjectFactory()).createBatProcessBatchRequestInputDetailInputObjectDetail();

            outputSpecification = (new ObjectFactory()).createBatProcessBatchRequestOutputSpecification();

            request.setBatchDetail(batchDetail);
            request.setBatchSpecification(batchSpecification);
            request.setInputDetail(inputDetail);
            request.setOutputSpecification(outputSpecification);

            batchSpecification.setBatchProcessID("CPCM_CM");
            batchSpecification.setBatchPriorityValue(5);
            batchSpecification.setBatchSpecificationObject(batchSpecificationObject);

            batchSpecificationObject.setLayoutName("Company Service");
            batchSpecificationObject.setObjectFormatTypeText("XML");

            inputDetail.setInputObjectDetail(inputObjectDetail);
            inputObjectDetail.setObjectFormatTypeText("CSV");
            inputObjectDetail.setLayoutName("PCMGBIMatch");
            inputObjectDetail.setCompressTypeValue("None");
            inputObjectDetail.setLanguageCode(36);
            inputObjectDetail.setLayoutVersion(1.0);

            outputSpecification.setObjectFormatTypeText("CSV");

        }

        public void setBatchDetail(String applicationBatchId, String messageTimeStamp) {
            batchDetail.setApplicationBatchID(applicationBatchId);
            batchDetail.setMessageTimeStamp(messageTimeStamp);
        }

        // contentObject should be base64 encrypted
        public void setBatchSpecificationObject(String contentObject) {
            batchSpecificationObject.setContentObject(contentObject);
        }

        public void setInputObjectDetail(String contentObject, int recordsCount) {
            inputObjectDetail.setContentObject(contentObject);
            inputObjectDetail.setRecordsCount(recordsCount);
        }

        public String getRequestBody() {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try {
                JAXBContext jaxbContext = JAXBContext
                        .newInstance("com.latticeengines.domain.exposed.datacloud.match.dnbbulkrequest");
                Marshaller marshaller = jaxbContext.createMarshaller();
                marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
                marshaller.marshal(request, outputStream);

            } catch (JAXBException e) {
                log.error(e);
            }
            return outputStream.toString();
        }
    }

}