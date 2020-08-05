package com.latticeengines.cdl.workflow.steps.importdata;


import static java.util.stream.Collectors.groupingBy;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.csv.CSVConstants;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.common.exposed.util.CompressionUtils.CompressType;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.cdl.S3ImportEmailInfo;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.PrepareImportConfiguration;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.serviceflows.workflow.report.BaseReportStep;

@Component("prepareImport")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PrepareImport extends BaseReportStep<PrepareImportConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(PrepareImport.class);

    private static final String INPUT_ROOT = "/atlas/rawinput";
    private static final String IN_PROGRESS = "/inprogress";
    private static final String COMPLETED = "/completed";
    private static final String FAILED = "/failed";

    private static final long GB = 1024L * 1024L * 1024L;

    @Inject
    private S3Service s3Service;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private TenantService tenantService;

    @Value("${cdl.s3.import.failed.tag}")
    private String s3ImportFailedTag;

    @Value("${cdl.s3.import.failed.tag.value}")
    private String s3ImportFailedTagValue;

    @Value("${cdl.s3.import.backup.tag}")
    private String importBackupTag;

    @Value("${cdl.s3.import.backup.tag.value}")
    private String importBackupTagValue;

    @Override
    public void execute() {
        copyToSystemFolder();
        validateFile();
    }

    @Override
    protected ReportPurpose getPurpose() {
        return ReportPurpose.PREPARE_IMPORT_SUMMARY;
    }

    private void validateFile() {
        String customerSpace = configuration.getCustomerSpace().toString();
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, configuration.getDataFeedTaskId());
        S3ImportEmailInfo emailInfo = configuration.getEmailInfo();
        Table template = dataFeedTask.getImportTemplate();
        String s3Bucket = configuration.getDestBucket();
        String s3FilePath = configuration.getDestKey();
        boolean needUpdateTask = false;
        List<String> warnings = new ArrayList<>();
        checkFileSize(s3Bucket, s3FilePath);
        CompressType compressType = CompressionUtils.getCompressType(s3Service.readObjectAsStream(s3Bucket, s3FilePath));
        try (InputStream fileStream = s3Service.readObjectAsStream(s3Bucket, s3FilePath)) {
            InputStream inputStream = CompressionUtils.getCompressInputStream(new BOMInputStream(fileStream, false,
                    ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE,
                    ByteOrderMark.UTF_32BE), compressType);
            InputStreamReader reader = null;
            if (inputStream instanceof ArchiveInputStream) {
                ArchiveEntry archiveEntry;
                ArchiveInputStream archiveInputStream = (ArchiveInputStream) inputStream;
                while ((archiveEntry = archiveInputStream.getNextEntry()) != null) {
                    if (CompressionUtils.isValidArchiveEntry(archiveEntry)) {
                        reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                        break;
                    }
                }
            } else {
                reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
            }
            if (reader == null) {
                throw new RuntimeException("Can't find valid input stream from uploaded file.");
            }
            CSVFormat format = LECSVFormat.format;
            CSVParser parser = new CSVParser(reader, format);
            Set<String> headerFields = parser.getHeaderMap().keySet();
            checkForCSVInjectionInFileNameAndHeaders(configuration.getSourceFileName(), headerFields);
            Map<String, Integer> longFieldMap = new HashMap<>();
            for (String field : headerFields) {
                if (StringUtils.length(field) > CSVConstants.MAX_HEADER_LENGTH) {
                    longFieldMap.put(field, StringUtils.length(field));
                }
            }
            if (MapUtils.isNotEmpty(longFieldMap)) {
                StringBuilder sb = new StringBuilder();
                longFieldMap.forEach((key, value) -> sb.append(String.format("\nfield: %s, length: %s", key, value)));
                throw new LedpException(LedpCode.LEDP_18188,
                        new String[] { String.valueOf(CSVConstants.MAX_HEADER_LENGTH), sb.toString() });
            }
            Map<String, String> headerCaseMapping = new HashMap<>();
            Set<String> duplicates = new HashSet<>();
            for (String field : headerFields) {
                if (headerCaseMapping.put(field.toLowerCase(), field) != null) {
                    duplicates.add(field);
                }
            }
            if (CollectionUtils.isNotEmpty(duplicates)) {
                Map<String, Object> paramsMap = ImmutableMap.of("columns", StringUtils.join(duplicates));
                throw new LedpException(LedpCode.LEDP_40055,  paramsMap);
            }
            Map<String, List<Attribute>> displayNameMap = template.getAttributes().stream()
                    .collect(groupingBy(attr -> attr.getSourceAttrName() == null ?
                            attr.getDisplayName() : attr.getSourceAttrName()));
            List<String> templateMissing = new ArrayList<>();
            List<String> csvMissing = new ArrayList<>();
            List<String> requiredMissing = new ArrayList<>();
            for (String header : headerFields) {
                if (!displayNameMap.containsKey(header)) {
                    templateMissing.add(header);
                }
            }
            for (Map.Entry<String, List<Attribute>> entry : displayNameMap.entrySet()) {
                if (!headerFields.contains(entry.getKey())) {
                    if (headerCaseMapping.containsKey(entry.getKey().toLowerCase())) { // case insensitive mapping.
                        for (Attribute attr : entry.getValue()) {
                            Set<String> possibleNames =
                                    CollectionUtils.isEmpty(template.getAttribute(attr.getName()).getPossibleCSVNames()) ?
                                            new HashSet<>() :
                                            new HashSet<>(template.getAttribute(attr.getName()).getPossibleCSVNames());
                            needUpdateTask = possibleNames.add(headerCaseMapping.get(entry.getKey().toLowerCase()));
                            template.getAttribute(attr.getName()).setPossibleCSVNames(new ArrayList<>(possibleNames));
                        }
                    } else {
                        csvMissing.add(entry.getKey());
                        for (Attribute attr : entry.getValue()) {
                            if (attr.getRequired() && attr.getDefaultValueStr() == null) {
                                requiredMissing.add(entry.getKey());
                            }
                        }
                    }
                }
            }
            if (CollectionUtils.isNotEmpty(templateMissing)) {
                String warningMsg = String.format("Template doesn't contains the following columns: %s",
                        String.join(",", templateMissing));
                log.warn(warningMsg);
                warnings.add(warningMsg);
            }
            if (CollectionUtils.isNotEmpty(csvMissing)) {
                String warningMsg = String.format("S3File doesn't contains the following columns: %s",
                        String.join(",", csvMissing));
                log.warn(warningMsg);
                warnings.add(warningMsg);
            }
            if (CollectionUtils.isNotEmpty(requiredMissing)) {
                throw new LedpException(LedpCode.LEDP_40043, new String[] { String.join(",", requiredMissing) });
            }
            parser.close();
            if (needUpdateTask) {
                log.info("Attribute possible name changed, so need to udpate data feed task.");
                dataFeedTask.setImportTemplate(template);
                dataFeedProxy.updateDataFeedTask(customerSpace, dataFeedTask);
            }
            String message = CollectionUtils.isNotEmpty(warnings) ? String.join("\n", warnings) : null;
            putOutputValue(WorkflowContextConstants.Outputs.IMPORT_WARNING, message);
            emailInfo.setErrorMsg(message);
            sendS3ImportEmail("In_Progress", emailInfo);
        } catch (LedpException e) {
            moveFromInProgressToFailed(s3FilePath);
            emailInfo.setErrorMsg(e.getMessage());
            sendS3ImportEmail("Failed", emailInfo);
            throw e;
        } catch (IOException e) {
            log.error(e.getMessage());
            sendS3ImportEmail("Failed", emailInfo);
            throw new RuntimeException("IO error! " + e.getMessage());
        } catch (IllegalArgumentException e) {
            moveFromInProgressToFailed(s3FilePath);
            emailInfo.setErrorMsg(e.getMessage());
            sendS3ImportEmail("Failed", emailInfo);
            log.error(e.getMessage());
            // PLS-13589 duplicate error will be threw by csv parser 491
            String errorMessage = e.getMessage();
            if (errorMessage.startsWith("The header contains a duplicate name:")) {
                throw new IllegalArgumentException(errorMessage.substring(0, errorMessage.indexOf(" in ")));
            } else {
                throw e;
            }
        } catch (Exception e) {
            log.error("Unknown Exception when validate S3 import! " + e.toString());
            moveFromInProgressToFailed(s3FilePath);
            emailInfo.setErrorMsg(e.getMessage());
            sendS3ImportEmail("Failed", emailInfo);
            throw e;
        }
    }

    private void checkFileSize(String s3Bucket, String fileKey) {
        ObjectMetadata objectMetadata = s3Service.getObjectMetadata(s3Bucket, fileKey);
        if (objectMetadata == null) {
            throw new RuntimeException(String.format("Cannot get metadata for file %s : %s", s3Bucket, fileKey));
        }
        if (objectMetadata.getContentLength() > 100L * GB) {
            throw new LedpException(LedpCode.LEDP_40078);
        }
    }

    private void sendS3ImportEmail(String result, S3ImportEmailInfo emailInfo) {
        try {
            String tenantId = configuration.getCustomerSpace().toString();
            emailProxy.sendS3ImportEmail(result, tenantId, emailInfo);
        } catch (Exception e) {
            log.error("Failed to send s3 import email: " + e.getMessage());
        }
    }

    private void checkForCSVInjectionInFileNameAndHeaders(String fileDisplayName, Set<String> headers) {
        if (CSVConstants.CSV_INJECTION_CHARACHTERS.indexOf(fileDisplayName.charAt(0)) != -1) {
            throw new LedpException(LedpCode.LEDP_18208);
        }
        for (String header : headers) {
            if (StringUtils.isNotBlank(header) && CSVConstants.CSV_INJECTION_CHARACHTERS.indexOf(header.charAt(0)) != -1) {
                throw new LedpException(LedpCode.LEDP_18208);
            }
        }
    }

    private void copyToSystemFolder() {
        String sourceKey = configuration.getSourceKey();
        String sourceBucket = configuration.getSourceBucket();
        String targetBucket = configuration.getDestBucket();
        String target = configuration.getDestKey();
        RetryTemplate retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AmazonS3Exception.class), null);
        retry.execute(context -> {
            if (context.getRetryCount() > 0) {
                log.info(String.format("(Attempt=%d) Retry copying object from %s:%s to %s:%s", //
                        context.getRetryCount() + 1, sourceBucket, sourceKey, targetBucket, target));
            }
            s3Service.copyObject(sourceBucket, sourceKey, targetBucket, target);
            log.info(String.format("(Attempt=%d) Retry add tag %s:%s to target %s:%s", context.getRetryCount() + 1,
                    importBackupTag, importBackupTagValue, targetBucket, target));
            s3Service.addTagToObject(targetBucket, target, importBackupTag, importBackupTagValue);
            return true;
        });
    }

    private void moveFromInProgressToFailed(String key) {
        if (!key.contains(IN_PROGRESS)) {
            return;
        }
        s3Service.addTagToObject(configuration.getDestBucket(), key, s3ImportFailedTag, s3ImportFailedTagValue);
    }

    private String getFileName(String key) {
        if (StringUtils.isEmpty(key) || key.lastIndexOf('/') < 0) {
            return key;
        }
        return key.substring(key.lastIndexOf('/') + 1);
    }

    private String[] getParts(String key) {
        while (key.startsWith("/")) {
            key = key.substring(1);
        }
        return key.split("/");
    }
}
