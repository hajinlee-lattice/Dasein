package com.latticeengines.apps.cdl.service.impl;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.MockBrokerInstanceService;
import com.latticeengines.apps.cdl.service.MockBrokerJobService;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.MockBrokerInstance;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;

import au.com.bytecode.opencsv.CSVWriter;

@Component("mockBrokerJobService")
public class MockBrokerJobServiceImpl implements MockBrokerJobService {

    private static final Logger log = LoggerFactory.getLogger(MockBrokerJobServiceImpl.class);

    @Inject
    private MockBrokerInstanceService mockBrokerInstanceService;

    @Inject
    private S3Service s3Service;

    @Value("${aws.s3.data.stage.bucket}")
    private String dataStageBucket;

    private String prefix = "enterprise_integration/";
    private String fileDir = "/tmp/";

    private int maxRow = 5;
    private int recordSize = 1000;
    private int batchSize = 200;

    private List<String> accountIds = new ArrayList<>();
    private List<String> contactIds = new ArrayList<>();
    private Random random = new Random(System.currentTimeMillis());

    private static CSVWriter createCSVWritter(FileWriter writer) {
        char separator = ',';
        char quotechar = '"';
        char escapechar = '\\';
        String lineEnd = "\n";
        return new CSVWriter(writer, separator, quotechar, escapechar, lineEnd);
    }

    @Override
    public Boolean generateMockFiles() {
        log.info("Start to generate mock file.");
        List<MockBrokerInstance> mockBrokerInstances = mockBrokerInstanceService.getAllInstance(maxRow);
        if (CollectionUtils.isNotEmpty(mockBrokerInstances)) {
            for (MockBrokerInstance mockBrokerInstance : mockBrokerInstances) {
                generateCSVFile(mockBrokerInstance);
            }
        }
        return true;
    }

    private void createTmpDir(String dir) {
        File file = new File(dir);
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    private void generateCSVFile(MockBrokerInstance mockBrokerInstance) {
        String sourceId = mockBrokerInstance.getSourceId();
        Tenant tenant = mockBrokerInstance.getTenant();
        CustomerSpace space = CustomerSpace.parse(tenant.getId());
        String tenantId = space.getTenantId();
        String documentType = mockBrokerInstance.getDocumentType();
        log.info(String.format("Generate mock file for documentType %s with source id %s.", documentType, sourceId));
        String fileName = documentType + "_" + UUID.randomUUID().toString() + ".csv";
        String separator = HdfsToS3PathBuilder.PATH_SEPARATOR;
        String subDir = tenantId + separator + sourceId + separator + documentType;
        String key = prefix + subDir + separator + fileName;
        createTmpDir(fileDir + subDir);
        File csvFile = new File(fileDir + subDir, fileName);
        List<String> fieldNames = mockBrokerInstance.getSelectedFields();
        if (CollectionUtils.isEmpty(fieldNames)) {
            log.info(String.format("Empty selected fields for documentType %s, skip generating CSV file.", documentType));
            return;
        }
        uploadCSVFileToS3(documentType, csvFile, fieldNames, dataStageBucket, key);
    }

    private void uploadCSVFileToS3(String documentType, File csvFile, List<String> fieldNames, String bucket, String key) {
        try {
            BusinessEntity entity = BusinessEntity.valueOf(documentType);
            boolean successCreated = true;
            try (CSVWriter csvWriter = createCSVWritter(new FileWriter(csvFile))) {
                csvWriter.writeNext(fieldNames.toArray(new String[0]));
                List<String[]> records = new ArrayList<>();
                for (int i = 0; i < recordSize; i++) {
                    if (i > 0 && i % batchSize == 0) {
                        csvWriter.writeAll(records);
                        csvWriter.flush();
                        records = new ArrayList<>();
                    }
                    records.add(generateRecord(entity, fieldNames));
                }
                csvWriter.writeAll(records);
            } catch (IOException e) {
                successCreated = false;
                log.error("Error happened when create csv file: ", e);
            }
            if (successCreated) {
                s3Service.uploadLocalFile(bucket, key, csvFile, true);
            }
        } finally {
            FileUtils.deleteQuietly(csvFile);
        }
    }

    private void addId(List<String> idList, String id) {
        if (idList.size() > recordSize) {
            idList.remove(0);
        }
        idList.add(id);
    }

    private String getRandomPhoneNumber() {
        StringBuffer buffer = new StringBuffer();
        buffer.append(random.nextInt(10));
        buffer.append(random.nextInt(10));
        buffer.append(random.nextInt(10));
        buffer.append(random.nextInt(10));
        return buffer.toString();
    }

    private String[] generateRecord(BusinessEntity entity, List<String> fieldNames) {
        String[] record = new String[fieldNames.size()];
        int index = 0;
        for (String fieldName : fieldNames) {
            switch (fieldName) {
                case "AccountId":
                    switch (entity) {
                        case Contact:
                            record[index] = UUID.randomUUID().toString();
                            break;
                        default:
                            String accountId;
                            boolean usedPrevious = random.nextBoolean();
                            if (usedPrevious && CollectionUtils.isNotEmpty(accountIds)) {
                                accountId = accountIds.get(random.nextInt(accountIds.size()));
                                record[index] = accountId;
                            } else {
                                accountId = UUID.randomUUID().toString();
                                record[index] = accountId;
                                accountIds.add(accountId);
                                addId(accountIds, accountId);
                            }
                            break;
                    }
                    break;
                case "ContactId":
                    String contactId;
                    boolean usedPrevious = random.nextBoolean();
                    if (usedPrevious && CollectionUtils.isNotEmpty(contactIds)) {
                        contactId = contactIds.get(random.nextInt(contactIds.size()));
                        record[index] = contactId;
                    } else {
                        contactId = UUID.randomUUID().toString();
                        record[index] = contactId;
                        contactIds.add(contactId);
                        addId(contactIds, contactId);
                    }
                    break;
                case "CompanyName":
                    record[index] = "IBM Corporation";
                    break;
                case "City":
                    record[index] = "Boston";
                    break;
                case "Country":
                    record[index] = "United States";
                    break;
                case "PhoneNumber":
                    record[index] = "+1 (608) 395-" + getRandomPhoneNumber();
                    break;
                case "Industry":
                    record[index] = "Business Services";
                    break;
                case "Website":
                    record[index] = "www.microstrategy.com";
                    break;
                case "Email":
                    record[index] = "testuser@lattice-engines.com";
                    break;
                case "FirstName":
                    record[index] = "testF";
                    break;
                case "LastName":
                    record[index] = "testL";
                    break;
                case "Title":
                    record[index] = "Senior IT Security Officer";
                    break;
                default:
                    break;
            }
            index++;
        }
        return record;
    }
}
