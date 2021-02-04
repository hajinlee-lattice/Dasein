package com.latticeengines.dcp.workflow.steps;

import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.Classification;
import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.ConfidenceCode;
import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.MatchedDuns;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.datacloud.MatchCoreErrorConstants;
import com.latticeengines.domain.exposed.datacloud.manage.PrimeColumn;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.dcp.DownloadFileType;
import com.latticeengines.domain.exposed.dcp.DunsCountCache;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.UploadStats;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.ImportSourceStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.dcp.SplitImportMatchResultConfig;
import com.latticeengines.domain.exposed.util.UploadS3PathBuilderUtils;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.proxy.exposed.dcp.DataReportProxy;
import com.latticeengines.proxy.exposed.dcp.ProjectProxy;
import com.latticeengines.proxy.exposed.dcp.SourceProxy;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;
import com.latticeengines.proxy.exposed.matchapi.PrimeMetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.dcp.SplitImportMatchResultJob;

import au.com.bytecode.opencsv.CSVReader;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SplitImportMatchResult extends RunSparkJob<ImportSourceStepConfiguration, SplitImportMatchResultConfig> {

    private static final Logger log = LoggerFactory.getLogger(SplitImportMatchResult.class);

    private static final List<String> BASE_SCHEMA = Arrays.asList(
            "duns_number", //
            "primaryname", //
            "countryisoalpha2code" //
    );

    @Inject
    private UploadProxy uploadProxy;

    @Inject
    private SourceProxy sourceProxy;

    @Inject
    private ProjectProxy projectProxy;

    @Inject
    private S3Service s3Service;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    private DataReportProxy dataReportProxy;

    @Inject
    private PrimeMetadataProxy primeMetadataProxy;

    @Value("${datacloud.manage.url}")
    private String url;

    @Value("${datacloud.manage.user}")
    private String user;

    @Value("${datacloud.manage.password.encrypted}")
    private String password;

    private Map<String, String> dataBlockDispNames = new HashMap<>();
    private Map<String, String> dataBlockBlockNames = new HashMap<>();

    @Override
    protected Class<SplitImportMatchResultJob> getJobClz() {
        return SplitImportMatchResultJob.class;
    }

    @Override
    protected SplitImportMatchResultConfig configureJob(ImportSourceStepConfiguration stepConfiguration) {
        String matchResultName = getStringValueFromContext(MATCH_RESULT_TABLE_NAME);
        Table matchResult = metadataProxy.getTable(configuration.getCustomerSpace().toString(), matchResultName);
        HdfsDataUnit input = matchResult.toHdfsDataUnit("input");
        SplitImportMatchResultConfig jobConfig = new SplitImportMatchResultConfig();
        jobConfig.setInput(Collections.singletonList(input));
        jobConfig.setTotalCount(input.getCount());

        jobConfig.setClassificationAttr(Classification);
        jobConfig.setMatchedDunsAttr(MatchedDuns);
        jobConfig.setCountryAttr(InterfaceName.Country.name());
        jobConfig.setManageDbUrl(url);
        jobConfig.setUser(user);
        String encryptionKey = CipherUtils.generateKey();
        jobConfig.setEncryptionKey(encryptionKey);
        String saltHint = CipherUtils.generateKey();
        jobConfig.setSaltHint(saltHint);
        jobConfig.setPassword(CipherUtils.encrypt(password, encryptionKey, saltHint));
        jobConfig.setConfidenceCodeAttr(ConfidenceCode);

        jobConfig.setErrorIndicatorAttr(MatchConstants.MATCH_ERROR_TYPE);
        jobConfig.setErrorCodeAttr(MatchConstants.MATCH_ERROR_CODE);
        jobConfig.setIgnoreErrors(stepConfiguration.getSuppressErrors() ? MatchCoreErrorConstants.IGNORE_ERRORS : Collections.emptyMap());

        List<ColumnMetadata> cms = matchResult.getColumnMetadata();
        List<PrimeColumn> primeColumns = getPrimeColumns();
        dataBlockDispNames = dataBlockFieldDisplayNames(primeColumns);
        dataBlockBlockNames = getBlockNames(primeColumns);
        log.info("InputSchema=" + JsonUtils.serialize(cms));
        List<ColumnMetadata> rejectedCms = cms.stream().filter(cm -> {
            boolean isCustomer = (cm.getTagList() == null) || !cm.getTagList().contains(Tag.EXTERNAL);
            boolean isIdToExclude = isAttrToExclude(cm);
            boolean isFromDataBlock = isFromDataBlock(cm);
            return isCustomer && !isIdToExclude && !isFromDataBlock;
        }).collect(Collectors.toList());
        List<String> rejectedAttrs = sortOutputAttrs(rejectedCms);
        jobConfig.setRejectedAttrs(rejectedAttrs);

        List<ColumnMetadata> acceptedCms = cms.stream() //
                .filter(cm -> !isAttrToExclude(cm) && !isProcessingErrorField(cm)).collect(Collectors.toList());
        Map<String, String> displayNameMap = convertToDispMap(cms);
        List<String> acceptedAttrs = sortOutputAttrs(acceptedCms);
        jobConfig.setAcceptedAttrs(acceptedAttrs);
        jobConfig.setDisplayNameMap(displayNameMap);

        log.info("JobConfig=" + JsonUtils.pprint(jobConfig));
        return jobConfig;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        String uploadId = configuration.getUploadId();
        UploadDetails upload = uploadProxy.getUploadByUploadId(customerSpace.toString(), uploadId, Boolean.TRUE);
        Source source = sourceProxy.getSource(customerSpace.toString(), configuration.getSourceId());
        ProjectDetails projectDetails = projectProxy.getDCPProjectByProjectId(customerSpace.toString(),
                configuration.getProjectId(), Boolean.FALSE, null);
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(customerSpace.toString());

        // create match result folder
        String dropFolder = UploadS3PathBuilderUtils.getDropFolder(dropBoxSummary.getDropBox());
        String uploadTS = upload.getUploadConfig().getUploadTimestamp();
        String uploadMatchResultDir = UploadS3PathBuilderUtils.getUploadMatchResultDir(projectDetails.getProjectId(),
                source.getSourceId(), upload.getDisplayName(), uploadTS);
        upload.getUploadConfig().setUploadMatchResultPrefix(uploadMatchResultDir);
        String matchResultPath = UploadS3PathBuilderUtils.combinePath(false, false, dropFolder, uploadMatchResultDir);
        if (!s3Service.objectExist(dropBoxSummary.getBucket(), matchResultPath)) {
            s3Service.createFolder(dropBoxSummary.getBucket(), matchResultPath);
            uploadProxy.updateUploadConfig(customerSpace.toString(), uploadId, upload.getUploadConfig());
        }

        // Copy files from spark workspace to upload result location.
        // Order must match the output order from Scala (matched -> unmatched -> processing errors -> DUNS count)
        Iterator<HdfsDataUnit> targetIt = result.getTargets().iterator();
        Set<DownloadFileType> generatedFiles = EnumSet.noneOf(DownloadFileType.class);
        if (copySparkCsvToS3(targetIt.next(), dropFolder, upload.getUploadConfig().getUploadMatchResultAccepted())) {
            generatedFiles.add(DownloadFileType.MATCHED);
        }
        if (copySparkCsvToS3(targetIt.next(), dropFolder, upload.getUploadConfig().getUploadMatchResultRejected())) {
            generatedFiles.add(DownloadFileType.UNMATCHED);
        }
        if (copySparkCsvToS3(targetIt.next(), dropFolder, upload.getUploadConfig().getUploadMatchResultErrored())) {
            generatedFiles.add(DownloadFileType.PROCESS_ERRORS);
        }
        updateDunsCount(targetIt.next(), uploadId);

        upload.getUploadConfig().getDownloadableFiles().addAll(generatedFiles);
        uploadProxy.updateUploadConfig(customerSpace.toString(), uploadId, upload.getUploadConfig());
        uploadProxy.updateUploadStatus(customerSpace.toString(), uploadId, Upload.Status.MATCH_FINISHED, null);
        updateUploadStatistics(result);
    }

    /*
     * Copies CSV from HDFS Data Unit (output from Spark job) to a location in S3
     *
     * Returns true if file exists (and was moved), otherwise false
     */
    private boolean copySparkCsvToS3(HdfsDataUnit sparkOutput, String dropFolder, String outputS3Path) {
        String sparkOutputFilePath = getCsvFilePath(sparkOutput);
        String targetPathForCopy = UploadS3PathBuilderUtils.combinePath(false, false, dropFolder, outputS3Path);
        boolean fileExists = StringUtils.isNotEmpty(sparkOutputFilePath);

        if (fileExists) {
            try {
                copyToS3(sparkOutputFilePath, targetPathForCopy);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return fileExists;
    }

    private void updateDunsCount(HdfsDataUnit unit, String uploadId) {
        // register duns count cache
        String dunsCountTableName = NamingUtils.timestamp("dunsCount");
        Table dunsCount = toTable(dunsCountTableName, null, unit);
        metadataProxy.createTable(configuration.getCustomerSpace().toString(), dunsCountTableName, dunsCount);
        DunsCountCache cache = new DunsCountCache();
        cache.setSnapshotTimestamp(new Date());
        cache.setDunsCountTableName(dunsCountTableName);
        dataReportProxy.registerDunsCount(configuration.getCustomerSpace().toString(), DataReportRecord.Level.Upload,
                uploadId, cache);
        // set table name to variable, then set table policy forever at finish step
        putStringValueInContext(DUNS_COUNT_TABLE_NAME, dunsCountTableName);
    }

    private void updateUploadStatistics(SparkJobResult result) {
        UploadStats stats = getObjectFromContext(UPLOAD_STATS, UploadStats.class);
        UploadStats.MatchStats matchStats = new UploadStats.MatchStats();
        long matchedCnt = result.getTargets().get(0).getCount();
        matchStats.setMatched(matchedCnt);
        long unmatchedCnt = stats.getImportStats().getSuccessfullyIngested() - matchStats.getMatched();
        matchStats.setUnmatched(unmatchedCnt);
        matchStats.setPendingReviewCnt(0L);
        //matchStats.setRejectedCnt(result.getTargets().get(1).getCount());
        stats.setMatchStats(matchStats);
        putObjectInContext(UPLOAD_STATS, stats);
        DataReport report = JsonUtils.deserialize(result.getOutput(), DataReport.class);

        // set matched/unmatched count for report
        DataReport.MatchToDUNSReport matchToDUNSReport = report.getMatchToDUNSReport();
        matchToDUNSReport.setMatched(matchedCnt);
        matchToDUNSReport.setUnmatched(unmatchedCnt);
        dataReportProxy.updateDataReport(configuration.getCustomerSpace().toString(), DataReportRecord.Level.Upload,
                configuration.getUploadId(), report);
    }

    private String getCsvFilePath(HdfsDataUnit dataUnit) {
        if (dataUnit.getCount() == 0) {
            return null;
        } else {
            return getFirstCsvFilePath(dataUnit);
        }
    }

    private String getUploadS3Path() {
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        String uploadId = configuration.getUploadId();
        UploadDetails upload = uploadProxy.getUploadByUploadId(customerSpace.toString(), uploadId, Boolean.TRUE);
        return upload.getUploadConfig().getUploadRawFilePath();
    }

    private List<String> getCustomerFileHeaders() {
        List<String> headers = new ArrayList<>();
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(customerSpace.toString());
        String filePath = getUploadS3Path();

        if (s3Service.objectExist(dropBoxSummary.getBucket(), filePath)) {
            InputStream inputStream = s3Service.readObjectAsStream(dropBoxSummary.getBucket(), filePath);
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            try (CSVReader csvReader = new CSVReader(inputStreamReader)) {
                String[] nextRecord = csvReader.readNext();
                headers.addAll(Arrays.asList(nextRecord));
            } catch (IOException e) {
                log.error("Error reading S3 file", e);
            }
        } else {
            log.error("Import file does not exist at the given path " + filePath);
        }
        return headers;
    }

    private List<ColumnMetadata> sortCustomerAttrs(List<ColumnMetadata> attrs) {
        List<String> customerHeaders = getCustomerFileHeaders();

        int sortedAttrsSize = attrs.size();
        if (customerHeaders.size() > sortedAttrsSize) {
            sortedAttrsSize = customerHeaders.size();
        }

        List<ColumnMetadata> sortedCustomerAttrs = new ArrayList<>(sortedAttrsSize);
        for(int index = 0; index < sortedAttrsSize; index++) {
            sortedCustomerAttrs.add(index, null);
        }

        List<ColumnMetadata> otherAttrs = new ArrayList<>();

        attrs.stream().forEach(columnMetadata -> {
            int index = customerHeaders.indexOf(columnMetadata.getDisplayName());
            if (index == -1) {
                otherAttrs.add(columnMetadata);
            } else {
                sortedCustomerAttrs.set(index, columnMetadata);
            }
        });

        sortedCustomerAttrs.removeIf(columnMetadata -> columnMetadata == null);
        sortedCustomerAttrs.addAll(otherAttrs);
        return sortedCustomerAttrs;
    }

    private List<String> sortOutputAttrs(Collection<ColumnMetadata> cms) {
        Map<String, String> candidateFieldDispNames = candidateFieldDisplayNames();
        List<ColumnMetadata> customerAttrs = new ArrayList<>();
        LinkedHashMap<String, ColumnMetadata> dataBlockAttrMap = new LinkedHashMap<>();
        List<ColumnMetadata> candidateAttrs = new ArrayList<>();
        List<ColumnMetadata> otherAttrs = new ArrayList<>();
        // MatchedDuns belongs to candidate attribute
        ColumnMetadata duns = null;
        for (ColumnMetadata cm : cms) {
            if (MatchedDuns.equals(cm.getAttrName())) {
                duns = cm;
            } else if (dataBlockDispNames.containsKey(cm.getAttrName())) {
                dataBlockAttrMap.put(cm.getAttrName(), cm);
            } else if (candidateFieldDispNames.containsKey(cm.getAttrName())) {
                candidateAttrs.add(cm);
            } else if ((cm.getTagList() == null) || !cm.getTagList().contains(Tag.EXTERNAL)){
                customerAttrs.add(cm);
            } else {
                otherAttrs.add(cm);
            }
        }
        // order base + enrichment columns
        List<ColumnMetadata> dataBlockAttrs = orderDataBlockAttributes(dataBlockAttrMap);

        List<String> attrNames = new ArrayList<>();
        List<ColumnMetadata> sortedCustomerAttrs = sortCustomerAttrs(customerAttrs);
        sortedCustomerAttrs.forEach(cm -> attrNames.add(cm.getAttrName()));
        attrNames.add(duns.getAttrName());
        candidateAttrs.forEach(cm -> attrNames.add(cm.getAttrName()));
        dataBlockAttrs.forEach(cm -> attrNames.add(cm.getAttrName()));
        otherAttrs.forEach(cm -> attrNames.add(cm.getAttrName()));
        return attrNames;
    }

    /*
     * Input: list of metadata to convert
     * Output: Map: metadata attrName -> CSV header
     */
    private Map<String, String> convertToDispMap(Collection<ColumnMetadata> cms) {
        Map<String, String> candidateFieldDispNames = candidateFieldDisplayNames();
        Map<String, String> dispNames = new LinkedHashMap<>();

        List<ColumnMetadata> customerAttrs = new ArrayList<>();
        List<ColumnMetadata> dataBlockAttrs = new ArrayList<>();
        List<ColumnMetadata> candidateAttrs = new ArrayList<>();
        List<ColumnMetadata> errorAttrs = new ArrayList<>();
        List<ColumnMetadata> otherAttrs = new ArrayList<>();
        // MatchedDuns belongs to candidate attribute
        ColumnMetadata duns = null;
        for (ColumnMetadata cm : cms) {
            if (MatchedDuns.equals(cm.getAttrName())) {
                duns = cm;
            } else if (dataBlockDispNames.containsKey(cm.getAttrName())) {
                dataBlockAttrs.add(cm);
            } else if (candidateFieldDispNames.containsKey(cm.getAttrName())) {
                candidateAttrs.add(cm);
            } else if (MatchCoreErrorConstants.CSV_HEADER_MAP.containsKey(cm.getAttrName())) {
                errorAttrs.add(cm);
            } else if ((cm.getTagList() == null) || !cm.getTagList().contains(Tag.EXTERNAL)){
                customerAttrs.add(cm);
            } else {
                otherAttrs.add(cm);
            }
        }
        customerAttrs.forEach(cm -> dispNames.put(cm.getAttrName(), cm.getDisplayName()));
        if (duns != null) {
            dispNames.put(MatchedDuns, candidateFieldDispNames.get(MatchedDuns));
        }
        candidateAttrs.forEach(cm -> dispNames.put(cm.getAttrName(), candidateFieldDispNames.get(cm.getAttrName())));
        dataBlockAttrs.forEach(cm -> dispNames.put(cm.getAttrName(), dataBlockDispNames.get(cm.getAttrName())));
        errorAttrs.forEach(cm -> dispNames.put(cm.getAttrName(), MatchCoreErrorConstants.CSV_HEADER_MAP.get(cm.getAttrName())));
        otherAttrs.forEach(cm -> dispNames.put(cm.getAttrName(), cm.getDisplayName()));
        log.info("the generated map are " + JsonUtils.serialize(dispNames));

        Map<String, List<String>> reversMap = dispNames.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, e -> new ArrayList<>(Collections.singleton(e.getKey())),
                        (l1, l2) -> {
                            CollectionUtils.addAll(l1, l2);
                            return l1;
                        }));
        reversMap.forEach((name, cols) -> {
            if (cols.size() > 1) {
                log.info("There are {} columns competing display name {}: {}", cols.size(), name, cols);
                if (cols.size() > 2) {
                    throw new RuntimeException("Do not know how to resolve display name conflict.");
                }
                List<String> customerCols = cols.stream() //
                        .filter(c -> !dataBlockDispNames.containsKey(c) && !candidateFieldDispNames.containsKey(c)) //
                        .collect(Collectors.toList());
                if (customerCols.size() == 1) {
                    String customerCol = customerCols.get(0);
                    String dispName = dispNames.get(customerCol);
                    String newDispName = "(Input) " + dispName;
                    dispNames.put(customerCol, newDispName);
                    log.info("Rename customer column [{}] to [{}]", dispName, newDispName);
                } else {
                    throw new RuntimeException("Do not know how to resolve display name conflict.");
                }
            }
        });
        return dispNames;
    }

    private boolean isAttrToExclude(ColumnMetadata cm) {
        return  Arrays.asList( //
                InterfaceName.InternalId.name(), //
                InterfaceName.CustomerAccountId.name(), //
                InterfaceName.LatticeAccountId.name(), //
                Classification //
        ).contains(cm.getAttrName()) || MatchConstants.matchDebugFields.contains(cm.getAttrName());
    }

    private boolean isFromDataBlock(ColumnMetadata cm) {
        return dataBlockDispNames.containsKey(cm.getAttrName());
    }

    private boolean isProcessingErrorField(ColumnMetadata cm) {
        return Arrays.asList(
                MatchConstants.MATCH_ERROR_CODE,
                MatchConstants.MATCH_ERROR_INFO,
                MatchConstants.MATCH_ERROR_TYPE
        ).contains(cm.getAttrName());
    }

    private Map<String, String> candidateFieldDisplayNames() {
        List<PrimeColumn> columns = primeMetadataProxy.getCandidateColumns();
        Map<String, String> dispNames = new HashMap<>();
        columns.forEach(c -> {
            dispNames.put(c.getPrimeColumnId(), c.getDisplayName());
        });
        return dispNames;
    }

    // to be changed to metadata driven
    private Map<String, String> dataBlockFieldDisplayNames(List<PrimeColumn> primeColumns) {
        Map<String, String> dispNames = new HashMap<>();
        for (PrimeColumn primeColumn: primeColumns) {
            dispNames.put(primeColumn.getPrimeColumnId(), primeColumn.getDisplayName());
        }
        return dispNames;
    }

    private Map<String, String> getBlockNames(List<PrimeColumn> primeColumns) {
        Map<String, String> blockNames = new HashMap<>();
        for (PrimeColumn primeColumn: primeColumns) {
            // TODO: don't hardcode access to the 0th datablock
            //       unsafe .get()?
            if (primeColumn.getDataBlocks() == null) {
                blockNames.put(primeColumn.getAttrName(), "missing datablocks");
            } else {
                blockNames.put(primeColumn.getAttrName(), primeColumn.getDataBlocks().get(0).getBlock());
            }
        }
        return blockNames;
    }

    private List<PrimeColumn> getPrimeColumns() {
        List<PrimeColumn> primeColumns = new ArrayList<>();
        List<String> elementIds = configuration.getAppendConfig().getElementIds();
        log.info("Start retrieving metadata for {} data block elements from match api", elementIds.size());
        List<String> chunk = new ArrayList<>();
        for (String elementId: elementIds) {
            chunk.add(elementId);
            if (chunk.size() >= 200) {
                List<PrimeColumn> primeChunk = primeMetadataProxy.getPrimeColumns(chunk);
                primeColumns.addAll(primeChunk);
                chunk.clear();
            }
        }
        if (!chunk.isEmpty()) {
            List<PrimeColumn> primeChunk = primeMetadataProxy.getPrimeColumns(chunk);
            primeColumns.addAll(primeChunk);
            chunk.clear();
        }
        log.info("Retrieved {} prime columns from match api", primeColumns.size());
        return primeColumns;
    }

    private List<ColumnMetadata> orderDataBlockAttributes(LinkedHashMap<String, ColumnMetadata> attrMap) {
        List<ColumnMetadata> orderedAttrs = new ArrayList<>();
        // when present, add schema (base info) values in order
        for (String e: BASE_SCHEMA) {
            if (attrMap.containsKey(e)) {
                orderedAttrs.add(attrMap.get(e));
                attrMap.remove(e);
            }
        }
        // add remaining attributes in attrMap, grouping by data block name
        LinkedHashMap<String, List<ColumnMetadata>> groups = new LinkedHashMap<>();
        String blockId;
        for (ColumnMetadata cm : attrMap.values()) {
            blockId = dataBlockBlockNames.get(cm.getAttrName());
            if (groups.containsKey(blockId)) {
                groups.get(blockId).add(cm);
            } else {
                List<ColumnMetadata> cms = new ArrayList<>();
                cms.add(cm);
                groups.put(blockId, cms);
            }
        }
        groups.keySet().forEach(block -> orderedAttrs.addAll(groups.get(block)));

        return orderedAttrs;
    }

}
