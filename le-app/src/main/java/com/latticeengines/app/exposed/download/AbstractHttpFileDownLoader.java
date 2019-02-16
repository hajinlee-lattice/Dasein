package com.latticeengines.app.exposed.download;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
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
import org.apache.commons.io.IOUtils;
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
                            GzipUtils.copyAndCompressStream(processDates(is), os);
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
        if (!shouldReformatDate) {
            return inputStream;
        }

        // todo: hard-coded date format. Need to be replaced in date attribute in phase 2.
        final String DATE_FORMAT = "MM/dd/yyyy hh:mm:ss a z";

        List<String> dateAttributes = getDateAttributes();
        if (CollectionUtils.isNotEmpty(dateAttributes)) {
            Map<String, String> dateToFormats = new HashMap<>();
            dateAttributes.forEach(attrib -> dateToFormats.put(attrib, DATE_FORMAT));
            return reformatDates(inputStream, dateToFormats);
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
                                    formatter.setTimeZone(TimeZone.getTimeZone("PST"));
                                    String dateInString = columnValue;
                                    try {
                                        dateInString = formatter.format(new Date(Long.valueOf(columnValue)));
                                    } catch (NumberFormatException exc) {
                                        // do nothing. Keep original date value as-is.
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
                                printer.print(val != null ? val: "");
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
            Map<BusinessEntity, List<AttrConfig>> customDisplayNameAttrs = cdlAttrConfigProxy
                    .getCustomDisplayNames(MultiTenantContext.getShortTenantId());
            if (MapUtils.isNotEmpty(customDisplayNameAttrs)
                    && CollectionUtils.isNotEmpty(customDisplayNameAttrs.get(BusinessEntity.Account))) {
                customDisplayNameAttrs.get(BusinessEntity.Account).forEach(config ->
                        nameToDisplayNameMap.put(config.getAttrName(),
                                (String) config.getProperty(ColumnMetadataKey.DisplayName).getCustomValue()));
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
