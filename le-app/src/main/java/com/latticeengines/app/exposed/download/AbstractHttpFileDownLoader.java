package com.latticeengines.app.exposed.download;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.FileCopyUtils;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.app.exposed.service.ImportFromS3Service;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.GzipUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public abstract class AbstractHttpFileDownLoader implements HttpFileDownLoader {
    private String mimeType;
    protected ImportFromS3Service importFromS3Service;
    private CDLAttrConfigProxy cdlAttrConfigProxy;
    private DataCollectionProxy dataCollectionProxy;
    private MetadataProxy metadataProxy;

    private static final Logger log = LoggerFactory.getLogger(AbstractHttpFileDownLoader.class);

    protected abstract String getFileName() throws Exception;

    protected abstract InputStream getFileInputStream() throws Exception;

    protected AbstractHttpFileDownLoader(String mimeType, ImportFromS3Service importFromS3Service) {
        this.mimeType = mimeType;
        this.importFromS3Service = importFromS3Service;
        this.cdlAttrConfigProxy = null;
        this.dataCollectionProxy = new DataCollectionProxy();
        this.metadataProxy = new MetadataProxy();
    }

    protected AbstractHttpFileDownLoader(String mimeType, ImportFromS3Service importFromS3Service,
            CDLAttrConfigProxy cdlAttrConfigProxy) {
        this.mimeType = mimeType;
        this.importFromS3Service = importFromS3Service;
        this.cdlAttrConfigProxy = cdlAttrConfigProxy;
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
                        GzipUtils.copyAndCompressStream(is, os);
                    }
                }
                break;
            case "application/csv":
                response.setHeader("Content-Disposition", String.format("attachment; filename=\"%s\"", //
                        StringUtils.substringBeforeLast(getFileName(), ".") + ".csv"));
            default:
                FileCopyUtils.copy(processInputStreamBasedOnMode(getFileInputStream(), mode),
                        response.getOutputStream());
                break;
            }
        } catch (Exception ex) {
            log.error("Failed to download file.", ex);
            throw new LedpException(LedpCode.LEDP_18022, ex);
        }
    }

    private InputStream processInputStreamBasedOnMode(InputStream inputStream, DownloadMode mode) {
        switch (mode) {
        case TOP_PREDICTOR:
            return processTopPredictorFile(inputStream);
        case RF_MODEL:
            return processRfModel(inputStream);
        case DEFAULT:
        default:
            return processDates(inputStream);
        }
    }

    private InputStream processDates(InputStream inputStream) {
        // todo: hard-coded date format. Need to be replaced in date attribute phase 2.
        final String DATE_FORMAT = "MM/dd/yyyy hh:mm:ss a z";

        List<String> dateAttributes = getDateAttributes();
        if (CollectionUtils.isNotEmpty(dateAttributes)) {
            Map<String, String> dateToFormats = new HashMap<>();
            dateAttributes.forEach(attrib -> { dateToFormats.put(attrib, DATE_FORMAT); });
            return reformatDates(inputStream, dateToFormats);
        }
        return inputStream;
    }

    private List<String> getDateAttributes() {
        List<String> dateAttributes = new ArrayList<>();
        DataCollection.Version activeVersion = dataCollectionProxy.getActiveVersion
                (MultiTenantContext.getShortTenantId());
        String accountTableName = dataCollectionProxy.getTableName(
                MultiTenantContext.getShortTenantId(),
                TableRoleInCollection.ConsolidatedAccount,
                activeVersion);
        String contactTableName = dataCollectionProxy.getTableName(
                MultiTenantContext.getShortTenantId(),
                TableRoleInCollection.ConsolidatedContact,
                activeVersion);
        List<ColumnMetadata> accountMetadata = metadataProxy.getTableColumns(
                MultiTenantContext.getShortTenantId(), accountTableName);
        List<ColumnMetadata> contactMetadata = metadataProxy.getTableColumns(
                MultiTenantContext.getShortTenantId(), contactTableName);
        List<ColumnMetadata> metadatas = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(accountMetadata)) {
            metadatas.addAll(accountMetadata);
        }
        if (CollectionUtils.isNotEmpty(contactMetadata)) {
            metadatas.addAll(contactMetadata);
        }
        for (ColumnMetadata metadata : metadatas) {
            if (metadata.getLogicalDataType().equals(LogicalDataType.Date)) {
                dateAttributes.add(metadata.getAttrName());
            }
        }

        return dateAttributes;
    }

    @VisibleForTesting
    InputStream reformatDates(InputStream inputStream, Map<String, String> dateToFormats) {
        log.info("Start to reformat date for tenant " + MultiTenantContext.getShortTenantId());

        StringBuilder sb = new StringBuilder();
        try (InputStreamReader reader = new InputStreamReader(
                new BOMInputStream(inputStream, false, ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE,
                        ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE),
                StandardCharsets.UTF_8)) {
            CSVFormat format = LECSVFormat.format;
            try (CSVParser parser = new CSVParser(reader, format)) {
                Map<String, Integer> headerMap = parser.getHeaderMap();
                try (CSVPrinter printer = new CSVPrinter(sb,
                        CSVFormat.DEFAULT.withHeader(parser.getHeaderMap().keySet().toArray(new String[]{})))) {
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
                                    formatter.setTimeZone(TimeZone.getTimeZone("PST"));
                                    String dateInString = formatter.format(new Date(Long.valueOf(columnValue)));
                                    recordAsArray[columnIndex] = dateInString;
                                }
                            }
                            for (String val : recordAsArray) {
                                printer.print(val != null ? String.valueOf(val) : "");
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
        } catch (IOException e) {
            log.error("Error reading the input stream.");
            e.printStackTrace();
            return inputStream;
        }

        return IOUtils.toInputStream(sb.toString(), Charset.defaultCharset());
    }

    private InputStream processRfModel(InputStream inputStream) {
        Map<String, String> nameToDisplayNameMap = getCustomizedDisplayNames();
        if (MapUtils.isNotEmpty(nameToDisplayNameMap)) {
            return fixRfModelDisplayName(inputStream, nameToDisplayNameMap);
        }
        return inputStream;
    }

    @VisibleForTesting
    InputStream fixRfModelDisplayName(InputStream inputStream, Map<String, String> nameToDisplayNameMap) {
        StringBuilder sb = new StringBuilder();

        log.info("start to replace rf model edit name for " + MultiTenantContext.getShortTenantId());
        try (InputStreamReader reader = new InputStreamReader(
                new BOMInputStream(inputStream, false, ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE,
                        ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE),
                StandardCharsets.UTF_8)) {

            CSVFormat format = LECSVFormat.format;
            try (CSVParser parser = new CSVParser(reader, format)) {
                parser.getHeaderMap().keySet().toArray();
                try (CSVPrinter printer = new CSVPrinter(sb,
                        CSVFormat.DEFAULT.withHeader(parser.getHeaderMap().keySet().toArray(new String[] {})))) {
                    for (CSVRecord record : parser) {
                        String attrName = record.get("Column Name");
                        if (attrName != null && nameToDisplayNameMap.containsKey(attrName)) {
                            String[] s = toArray(record);
                            log.info("replacing " + record.get("Column Name") + " with "
                                    + nameToDisplayNameMap.get(attrName));
                            s[2] = nameToDisplayNameMap.get(attrName);
                            for (String val : s) {
                                printer.print(val != null ? String.valueOf(val) : "");
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

        return IOUtils.toInputStream(sb.toString(), Charset.defaultCharset());
    }

    private Map<String, String> getCustomizedDisplayNames() {
        Map<String, String> nameToDisplayNameMap = new HashMap<>();
        try {
            List<AttrConfig> customDisplayNameAttrs = cdlAttrConfigProxy
                    .getCustomDisplayNames(MultiTenantContext.getShortTenantId());
            if (CollectionUtils.isNotEmpty(customDisplayNameAttrs)) {
                List<AttrConfig> renderedConfigList = cdlAttrConfigProxy
                        .renderConfigs(MultiTenantContext.getShortTenantId(), customDisplayNameAttrs);
                renderedConfigList.forEach(config -> {
                    nameToDisplayNameMap.put(config.getAttrName(),
                            config.getPropertyFinalValue(ColumnMetadataKey.DisplayName, String.class));
                });
            }
        } catch (LedpException e) {
            log.warn("Got LedpException " + ExceptionUtils.getStackTrace(e));
        }
        return nameToDisplayNameMap;
    }

    private InputStream processTopPredictorFile(InputStream inputStream) {
        Map<String, String> nameToDisplayNameMap = getCustomizedDisplayNames();
        if (MapUtils.isNotEmpty(nameToDisplayNameMap)) {
            return fixPredictorDisplayName(inputStream, nameToDisplayNameMap);
        }
        return inputStream;
    }

    @VisibleForTesting
    InputStream fixPredictorDisplayName(InputStream inputStream, Map<String, String> nameToDisplayNameMap) {
        StringBuilder sb = new StringBuilder();

        log.info("start to replace top predictor edit name for " + MultiTenantContext.getShortTenantId());
        try (InputStreamReader reader = new InputStreamReader(
                new BOMInputStream(inputStream, false, ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE,
                        ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE),
                StandardCharsets.UTF_8)) {

            CSVFormat format = LECSVFormat.format;
            try (CSVParser parser = new CSVParser(reader, format)) {
                parser.getHeaderMap().keySet().toArray();
                try (CSVPrinter printer = new CSVPrinter(sb,
                        CSVFormat.DEFAULT.withHeader(parser.getHeaderMap().keySet().toArray(new String[] {})))) {
                    for (CSVRecord record : parser) {
                        String attrName = record.get("Original Column Name");
                        if (attrName != null && nameToDisplayNameMap.containsKey(attrName)) {
                            String[] s = toArray(record);
                            log.info("replacing " + record.get("Attribute Name") + " with "
                                    + nameToDisplayNameMap.get(attrName));
                            s[1] = nameToDisplayNameMap.get(attrName);
                            for (String val : s) {
                                printer.print(val != null ? String.valueOf(val) : "");
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

        return IOUtils.toInputStream(sb.toString(), Charset.defaultCharset());
    }

    private String[] toArray(CSVRecord rec) {
        String[] arr = new String[rec.size()];
        int i = 0;
        for (String str : rec) {
            arr[i++] = str;
        }
        return arr;
    }

    @Override
    public void downloadFile(HttpServletRequest request, HttpServletResponse response) {
        downloadFile(request, response, DownloadMode.DEFAULT);
    }

}
