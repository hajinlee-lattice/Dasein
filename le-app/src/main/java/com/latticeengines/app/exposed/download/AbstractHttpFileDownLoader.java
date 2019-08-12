package com.latticeengines.app.exposed.download;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.FileCopyUtils;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.app.exposed.service.ImportFromS3Service;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.GzipUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public abstract class AbstractHttpFileDownLoader implements HttpFileDownLoader {
    private static final Logger log = LoggerFactory.getLogger(AbstractHttpFileDownLoader.class);

    private String mimeType;
    protected ImportFromS3Service importFromS3Service;
    private CDLAttrConfigProxy cdlAttrConfigProxy;
    private DataCollectionProxy dataCollectionProxy;
    private MetadataProxy metadataProxy;
    private BatonService batonService;
    private boolean shouldReformatDate = false;

    public void setShouldReformatDate(boolean shouldReformatDate) {
        this.shouldReformatDate = shouldReformatDate;
    }

    protected abstract String getFileName() throws Exception;

    protected abstract InputStream getFileInputStream() throws Exception;

    protected AbstractHttpFileDownLoader(String mimeType, ImportFromS3Service importFromS3Service,
            CDLAttrConfigProxy cdlAttrConfigProxy, BatonService batonService) {
        this.mimeType = mimeType;
        this.importFromS3Service = importFromS3Service;
        this.cdlAttrConfigProxy = cdlAttrConfigProxy;
        this.batonService = batonService;
        this.dataCollectionProxy = new DataCollectionProxy();
        this.metadataProxy = new MetadataProxy();
    }

    @Override
    public void downloadFile(HttpServletRequest request, HttpServletResponse response, DownloadMode mode) {
        try {
            response.setContentType(mimeType);
            response.setHeader("Content-Disposition", String.format("attachment; filename=\"%s\"", getFileName()));
            switch (mimeType) {
            case MediaType.APPLICATION_OCTET_STREAM:
                try (InputStream is = getFileInputStream()) {
                    try (OutputStream os = response.getOutputStream()) {
                        if (shouldReformatDate) {
                            StringBuilder sb = new StringBuilder();
                            GzipUtils.copyAndCompressStream(processDates(is, sb), os);
                            deleteFile(sb.toString());
                        } else {
                            GzipUtils.copyAndCompressStream(is, os);
                        }
                    }
                }
                break;
            case "application/csv":
                response.setHeader("Content-Disposition", String.format("attachment; filename=\"%s\"", //
                        StringUtils.substringBeforeLast(getFileName(), ".") + ".csv"));
            default:
                StringBuilder sb = new StringBuilder();
                FileCopyUtils.copy(processInputStreamBasedOnMode(getFileInputStream(), mode, sb),
                        response.getOutputStream());
                deleteFile(sb.toString());
                break;
            }
        } catch (Exception exc) {
            log.error("Failed to download file.", exc);
            throw new LedpException(LedpCode.LEDP_18022, exc);
        }
    }

    protected void deleteFile(String filename) {
        if (StringUtils.isNotBlank(filename)) {
            File file = new File(filename);
            try {
                FileUtils.forceDelete(file);
                log.info("Delete file " + filename);
            } catch (IOException exc) {
                log.warn("Cannot delete file " + filename);
            }
        }
    }

    private InputStream processInputStreamBasedOnMode(InputStream inputStream, DownloadMode mode,
            StringBuilder filenameBuilder) {
        switch (mode) {
        case TOP_PREDICTOR:
            return processTopPredictorFile(inputStream, filenameBuilder);
        case RF_MODEL:
            return processRfModel(inputStream, filenameBuilder);
        case DEFAULT:
        default:
            return processDates(inputStream, filenameBuilder);
        }
    }

    private InputStream processDates(InputStream inputStream, StringBuilder filenameBuilder) {
        if (!shouldReformatDate) {
            return inputStream;
        }

        // todo: hard-coded date format. Need to be replaced in date attribute
        // in phase 2.
        final String DATE_FORMAT = "MM/dd/yyyy hh:mm:ss a z";

        List<String> dateAttributes = getDateAttributes();
        log.info("dateAttributes=" + JsonUtils.serialize(dateAttributes));

        if (CollectionUtils.isNotEmpty(dateAttributes)) {
            Map<String, String> dateToFormats = new HashMap<>();
            dateAttributes.forEach(attrib -> dateToFormats.put(attrib, DATE_FORMAT));
            return reformatDates(inputStream, dateToFormats, filenameBuilder);
        }
        return inputStream;
    }

    private List<String> getDateAttributes() {
        List<String> dateAttributes = new ArrayList<>();
        String shortTenantId = MultiTenantContext.getShortTenantId();
        if (StringUtils.isBlank(shortTenantId)) {
            return Collections.emptyList();
        }

        if (batonService != null && !batonService.hasProduct(CustomerSpace.parse(shortTenantId), LatticeProduct.CG)) {
            return Collections.emptyList();
        }
        DataCollection.Version activeVersion = dataCollectionProxy.getActiveVersion(shortTenantId);
        String accountTableName = dataCollectionProxy.getTableName(shortTenantId,
                TableRoleInCollection.ConsolidatedAccount, activeVersion);
        String contactTableName = dataCollectionProxy.getTableName(shortTenantId,
                TableRoleInCollection.ConsolidatedContact, activeVersion);
        List<ColumnMetadata> accountMetadata = Collections.emptyList();
        List<ColumnMetadata> contactMetadata = Collections.emptyList();
        if (StringUtils.isNotEmpty(accountTableName)) {
            accountMetadata = metadataProxy.getTableColumns(shortTenantId, accountTableName);
        }
        if (StringUtils.isNotEmpty(contactTableName)) {
            contactMetadata = metadataProxy.getTableColumns(shortTenantId, contactTableName);
        }
        List<ColumnMetadata> metadatas = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(accountMetadata)) {
            metadatas.addAll(accountMetadata);
        }
        if (CollectionUtils.isNotEmpty(contactMetadata)) {
            metadatas.addAll(contactMetadata);
        }
        for (ColumnMetadata metadata : metadatas) {
            if (LogicalDataType.Date.equals(metadata.getLogicalDataType())) {
                dateAttributes.add(metadata.getDisplayName());
            }
        }

        return dateAttributes;
    }

    protected String tempFolderName() {
        String tmpdir = System.getProperty("java.io.tmpdir");
        if (tmpdir.endsWith("/")) {
            tmpdir = tmpdir.substring(0, tmpdir.length() - 1);
        }
        return new StringBuilder().append(tmpdir).append(NamingUtils.uuid("/download")).toString();
    }

    @VisibleForTesting
    InputStream reformatDates(InputStream inputStream, Map<String, String> dateToFormats,
            StringBuilder filenameBuilder) {
        String tenant = MultiTenantContext.getShortTenantId();
        log.info(String.format("dateToFormats=%s. Tenant=%s", JsonUtils.serialize(dateToFormats), tenant));
        filenameBuilder.append(tempFolderName());
        String filename = filenameBuilder.toString();
        log.info(String.format("Use temporary file=%s. Tenant=%s", filename, tenant));
        BufferedWriter bw;
        try {
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename), StandardCharsets.UTF_8));
        } catch (FileNotFoundException exc) {
            log.error(String.format("Error while opening the temporary file %s. Tenant=%s", filename, tenant), exc);
            return inputStream;
        }

        try (InputStreamReader reader = new InputStreamReader(
                new BOMInputStream(inputStream, false, ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE,
                        ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE),
                StandardCharsets.UTF_8)) {
            CSVFormat format = LECSVFormat.format;
            try (CSVParser parser = new CSVParser(reader, format)) {
                Map<String, Integer> headerMap = parser.getHeaderMap();
                try (CSVPrinter printer = new CSVPrinter(bw,
                        CSVFormat.DEFAULT.withHeader(parser.getHeaderMap().keySet().toArray(new String[] {})))) {
                    List<CSVRecord> recordsWithErrors = new ArrayList<>();
                    for (CSVRecord record : parser) {
                        try {
                            String[] recordAsArray = toArray(record);
                            for (Map.Entry<String, Integer> entry : headerMap.entrySet()) {
                                String columnName = entry.getKey();
                                Integer columnIndex = entry.getValue();
                                String columnValue = record.get(columnName);
                                if (StringUtils.isNotBlank(columnValue) && dateToFormats.containsKey(columnName)) {
                                    String dateFormat = dateToFormats.get(columnName);
                                    SimpleDateFormat formatter = new SimpleDateFormat(dateFormat);
                                    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
                                    String dateInString = columnValue;
                                    try {
                                        dateInString = formatter.format(new Date(Long.valueOf(columnValue)));
                                    } catch (NumberFormatException exc) {
                                        // do nothing. Keep original date value
                                        // as-is.
                                        log.info(String.format("Skip reformatting date value %s for column %s",
                                                columnValue, columnName));
                                    }
                                    recordAsArray[columnIndex] = dateInString;
                                }
                            }
                            for (String val : recordAsArray) {
                                printer.print(val != null ? val : "");
                            }
                            printer.println();
                        } catch (IOException exc) {
                            recordsWithErrors.add(record);
                        }
                    }

                    // deal with records having errors
                    for (CSVRecord record : recordsWithErrors) {
                        printer.printRecord(record);
                    }
                }
            }
        } catch (IOException exc) {
            log.error(String.format("Error reading the input stream. Tenant=%s", tenant), exc);
            return inputStream;
        }

        try {
            return new FileInputStream(filename);
        } catch (FileNotFoundException exc) {
            log.error(String.format("Error while reading temporary file %s. Tenant=%s", filename, tenant), exc);
            return inputStream;
        }
    }

    private InputStream processRfModel(InputStream inputStream, StringBuilder filenameBuilder) {
        Map<String, String> nameToDisplayNameMap = getCustomizedDisplayNames();
        if (MapUtils.isNotEmpty(nameToDisplayNameMap)) {
            return fixRfModelDisplayName(inputStream, nameToDisplayNameMap, filenameBuilder);
        }
        return inputStream;
    }

    @VisibleForTesting
    InputStream fixRfModelDisplayName(InputStream inputStream, Map<String, String> nameToDisplayNameMap,
            StringBuilder filenameBuilder) {

        String tenant = MultiTenantContext.getShortTenantId();
        log.info("start to replace rf model edit name for " + MultiTenantContext.getShortTenantId());
        filenameBuilder.append(tempFolderName());
        String filename = filenameBuilder.toString();
        log.info(String.format("Use temporary file=%s. Tenant=%s", filename, tenant));

        try (InputStreamReader reader = new InputStreamReader(
                new BOMInputStream(inputStream, false, ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE,
                        ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE),
                StandardCharsets.UTF_8)) {

            CSVFormat format = LECSVFormat.format;
            try (CSVParser parser = new CSVParser(reader, format)) {
                parser.getHeaderMap().keySet().toArray();
                try (CSVPrinter printer = new CSVPrinter(
                        new BufferedWriter(
                                new OutputStreamWriter(new FileOutputStream(filename), StandardCharsets.UTF_8)),
                        CSVFormat.DEFAULT.withHeader(parser.getHeaderMap().keySet().toArray(new String[] {})))) {
                    for (CSVRecord record : parser) {
                        String attrName = record.get("Column Name");
                        if (attrName != null && nameToDisplayNameMap.containsKey(attrName)) {
                            String[] s = toArray(record);
                            log.info("replacing " + record.get("Column Name") + " with "
                                    + nameToDisplayNameMap.get(attrName));
                            s[2] = nameToDisplayNameMap.get(attrName);
                            for (String val : s) {
                                printer.print(val != null ? val : "");
                            }
                            printer.println();
                        } else {
                            printer.printRecord(record);
                        }
                    }
                }
            }
        } catch (IOException e) {
            log.error("Error reading the input stream.");
            e.printStackTrace();
            return inputStream;
        }

        try {
            return new FileInputStream(filename);
        } catch (FileNotFoundException exc) {
            log.error(String.format("Error while reading temporary file %s. Tenant=%s", filename, tenant), exc);
            return inputStream;
        }
    }

    private Map<String, String> getCustomizedDisplayNames() {
        Map<String, String> nameToDisplayNameMap = new HashMap<>();
        try {
            Map<BusinessEntity, List<AttrConfig>> customDisplayNameAttrs = cdlAttrConfigProxy
                    .getCustomDisplayNames(MultiTenantContext.getShortTenantId());
            if (MapUtils.isNotEmpty(customDisplayNameAttrs)
                    && CollectionUtils.isNotEmpty(customDisplayNameAttrs.get(BusinessEntity.Account))) {
                customDisplayNameAttrs.get(BusinessEntity.Account)
                        .forEach(config -> nameToDisplayNameMap.put(config.getAttrName(),
                                (String) config.getProperty(ColumnMetadataKey.DisplayName).getCustomValue()));
            }
        } catch (LedpException e) {
            log.warn("Got LedpException " + ExceptionUtils.getStackTrace(e));
        }
        return nameToDisplayNameMap;
    }

    private InputStream processTopPredictorFile(InputStream inputStream, StringBuilder filenameBuilder) {
        Map<String, String> nameToDisplayNameMap = getCustomizedDisplayNames();
        if (MapUtils.isNotEmpty(nameToDisplayNameMap)) {
            return fixPredictorDisplayName(inputStream, nameToDisplayNameMap, filenameBuilder);
        }
        return inputStream;
    }

    @VisibleForTesting
    InputStream fixPredictorDisplayName(InputStream inputStream, Map<String, String> nameToDisplayNameMap,
            StringBuilder filenameBuilder) {

        String tenant = MultiTenantContext.getShortTenantId();
        log.info("start to replace rf model edit name for " + MultiTenantContext.getShortTenantId());
        filenameBuilder.append(tempFolderName());
        String filename = filenameBuilder.toString();
        log.info(String.format("Use temporary file=%s. Tenant=%s", filename, tenant));

        log.info("start to replace top predictor edit name for " + MultiTenantContext.getShortTenantId());
        try (InputStreamReader reader = new InputStreamReader(
                new BOMInputStream(inputStream, false, ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE,
                        ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE),
                StandardCharsets.UTF_8)) {

            CSVFormat format = LECSVFormat.format;
            try (CSVParser parser = new CSVParser(reader, format)) {
                parser.getHeaderMap().keySet().toArray();
                try (CSVPrinter printer = new CSVPrinter(
                        new BufferedWriter(
                                new OutputStreamWriter(new FileOutputStream(filename), StandardCharsets.UTF_8)),
                        CSVFormat.DEFAULT.withHeader(parser.getHeaderMap().keySet().toArray(new String[] {})))) {
                    for (CSVRecord record : parser) {
                        String attrName = record.get("Original Column Name");
                        if (attrName != null && nameToDisplayNameMap.containsKey(attrName)) {
                            String[] s = toArray(record);
                            log.info("replacing " + record.get("Attribute Name") + " with "
                                    + nameToDisplayNameMap.get(attrName));
                            s[1] = nameToDisplayNameMap.get(attrName);
                            for (String val : s) {
                                printer.print(val != null ? val : "");
                            }
                            printer.println();
                        } else {
                            printer.printRecord(record);
                        }
                    }
                }
            }
        } catch (IOException e) {
            log.error("Error reading the input stream.");
            e.printStackTrace();
            return inputStream;
        }

        try {
            return new FileInputStream(filename);
        } catch (FileNotFoundException exc) {
            log.error(String.format("Error while reading temporary file %s. Tenant=%s", filename, tenant), exc);
            return inputStream;
        }
    }

    private InputStream transformHeaderForCsv(InputStream inputStream, Map<String, String> headerTransformation,
                              StringBuilder filenameBuilder) {

        //initial preparation
        String tenant = MultiTenantContext.getShortTenantId();
        log.info(String.format("headerTransformation=%s. Tenant=%s", JsonUtils.serialize(headerTransformation), tenant));
        filenameBuilder.append(tempFolderName());
        String filename = filenameBuilder.toString();
        log.info(String.format("Use temporary file=%s. Tenant=%s", filename, tenant));

        //prepare output
        BufferedWriter bw;
        try {

            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename), StandardCharsets.UTF_8));

        } catch (FileNotFoundException exc) {

            log.error(String.format("Error while opening the temporary file %s. Tenant=%s", filename, tenant), exc);
            return inputStream;

        }

        //transform
        try (InputStreamReader reader = new InputStreamReader(
                new BOMInputStream(inputStream, false, ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE,
                        ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE),
                StandardCharsets.UTF_8)) {
            try (CSVParser parser = new CSVParser(reader, LECSVFormat.format)) {

                //prepare header
                Map<String, Integer> headerMap = parser.getHeaderMap();
                String[] newHeader = transformHeader(headerMap, headerTransformation);

                //output
                String[] outBuf = new String[newHeader.length];
                try (CSVPrinter printer = new CSVPrinter(bw,
                        CSVFormat.DEFAULT.withHeader(newHeader))) {

                    //loop the records
                    for (CSVRecord record : parser) {

                        //rec => string array
                        toArray(record, outBuf);

                        //output rec
                        printer.printRecord(outBuf);

                    }

                }

            }

        } catch (IOException exc) {

            log.error(String.format("Error reading the input stream. Tenant=%s", tenant), exc);
            return inputStream;

        }

        //return newly created stream
        try {

            return new FileInputStream(filename);

        } catch (FileNotFoundException exc) {

            log.error(String.format("Error while reading temporary file %s. Tenant=%s", filename, tenant), exc);
            return inputStream;

        }

    }

    private String[] toArray(CSVRecord rec) {
        String[] arr = new String[rec.size()];
        int i = 0;
        for (String str : rec) {
            arr[i++] = str;
        }
        return arr;
    }

    private void toArray(CSVRecord rec, String[] outBuf) {

        for (int i = 0; i < rec.size(); ++i) {
            outBuf[i] = rec.get(i);
        }

    }

    private String[] transformHeader(Map<String, Integer> headerMap, Map<String, String> transformation) {

        String[] newHeader = new String[headerMap.size()];
        for (Map.Entry<String, Integer> entry: headerMap.entrySet()) {

            String columnName = transformation.containsKey(entry.getKey()) ? transformation.get(entry.getKey()) : entry.getKey();
            newHeader[entry.getValue()] = columnName;

        }

        return newHeader;
    }

    @Override
    public void downloadFile(HttpServletRequest request, HttpServletResponse response) {
        downloadFile(request, response, DownloadMode.DEFAULT);
    }

    @Override
    public void downloadCsvWithTransform(HttpServletRequest request, HttpServletResponse response, Map<String, String> headerTransform) {

        List<String> tmpFiles = new ArrayList<>(2);
        List<InputStream> tmpStreams = new ArrayList<>(2);

        //only support octet_stream
        try {

            response.setContentType(MediaType.APPLICATION_OCTET_STREAM);
            response.setHeader("Content-Disposition", String.format("attachment; filename=\"%s\"", getFileName()));

            try (InputStream is = getFileInputStream()) {
                try (OutputStream os = response.getOutputStream()) {

                    InputStream curIs = is;

                    //reformat date?
                    if (shouldReformatDate) {

                        log.info("re-format dates...");
                        InputStream prevIs = curIs;
                        StringBuilder sb = new StringBuilder();
                        curIs = processDates(prevIs, sb);

                        if (curIs != prevIs) {

                            tmpFiles.add(sb.toString());
                            tmpStreams.add(curIs);

                        }

                    }

                    //transform header
                    if (headerTransform != null) {

                        log.info("transform header...");
                        InputStream prevIs = curIs;
                        StringBuilder sb = new StringBuilder();
                        curIs = transformHeaderForCsv(prevIs, headerTransform, sb);

                        if (curIs != prevIs) {

                            tmpFiles.add(sb.toString());
                            tmpStreams.add(curIs);

                        }

                    }

                    //send to response
                    log.info("send content...");
                    GzipUtils.copyAndCompressStream(curIs, os);

                    log.info("downloading training csv file is done");
                }
            }

        } catch (Exception exc) {

            log.error("Failed to download file.", exc);
            throw new LedpException(LedpCode.LEDP_18022, exc);

        } finally {

            for (InputStream stream : tmpStreams) {

                try {

                    stream.close();

                } catch (IOException e) {

                }

            }

            //delete temp files
            for (String tmpFile : tmpFiles) {

                deleteFile(tmpFile);

            }

        }

    }
}
