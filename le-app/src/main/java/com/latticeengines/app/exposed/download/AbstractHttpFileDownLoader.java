package com.latticeengines.app.exposed.download;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;

public abstract class AbstractHttpFileDownLoader implements HttpFileDownLoader {

    private String mimeType;
    protected ImportFromS3Service importFromS3Service;
    private CDLAttrConfigProxy cdlAttrConfigProxy;
    private static final Logger log = LoggerFactory.getLogger(AbstractHttpFileDownLoader.class);

    protected abstract String getFileName() throws Exception;

    protected abstract InputStream getFileInputStream() throws Exception;

    protected AbstractHttpFileDownLoader(String mimeType, ImportFromS3Service importFromS3Service) {
        this.mimeType = mimeType;
        this.importFromS3Service = importFromS3Service;
        this.cdlAttrConfigProxy = null;
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
            return inputStream;
        }
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
                StandardCharsets.UTF_8);) {

            CSVFormat format = LECSVFormat.format;
            try (CSVParser parser = new CSVParser(reader, format);) {
                parser.getHeaderMap().keySet().toArray();
                try (CSVPrinter printer = new CSVPrinter(sb,
                        CSVFormat.DEFAULT.withHeader(parser.getHeaderMap().keySet().toArray(new String[] {})));) {
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
                renderedConfigList.stream().forEach(config -> {
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
                StandardCharsets.UTF_8);) {

            CSVFormat format = LECSVFormat.format;
            try (CSVParser parser = new CSVParser(reader, format);) {
                parser.getHeaderMap().keySet().toArray();
                try (CSVPrinter printer = new CSVPrinter(sb,
                        CSVFormat.DEFAULT.withHeader(parser.getHeaderMap().keySet().toArray(new String[] {})));) {
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
