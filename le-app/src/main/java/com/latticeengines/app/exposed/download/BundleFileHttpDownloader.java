package com.latticeengines.app.exposed.download;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.app.exposed.service.ImportFromS3Service;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

public class BundleFileHttpDownloader extends AbstractHttpFileDownLoader {

    private static final Logger log = LoggerFactory.getLogger(BundleFileHttpDownloader.class);
    public static final String[] HEADERS = new String[] { "Product Id", "Product Name", "Product Bundle",
            "Description"};
    private String fileName;
    private String bucketName;
    private DataCollectionProxy dataCollectionProxy;
    private String podId;
    private Configuration configuration;
    private String tempFileName = null;

    public BundleFileHttpDownloader(BundleFileHttpDownloaderBuilder bundleFileHttpDownloaderBuilder) {
        super(bundleFileHttpDownloaderBuilder.mimeType, bundleFileHttpDownloaderBuilder.importFromS3Service, null,
                null);
        this.fileName = bundleFileHttpDownloaderBuilder.fileName;
        this.bucketName = bundleFileHttpDownloaderBuilder.bucketName;
        this.dataCollectionProxy = bundleFileHttpDownloaderBuilder.dataCollectionProxy;
        this.podId = bundleFileHttpDownloaderBuilder.podId;
        this.configuration = bundleFileHttpDownloaderBuilder.configuration;
    }

    @Override
    protected String getFileName() throws Exception {
        return this.fileName;
    }

    private Table getCurrentConsolidateProductTable(String customerSpace) {
        DataCollection.Version activeVersion = dataCollectionProxy.getActiveVersion(customerSpace);
        DataCollection.Version inactiveVersion = activeVersion.complement();
        Table currentTable = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedProduct, activeVersion);
        if (currentTable != null) {
            return currentTable;
        }

        currentTable = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedProduct,
                inactiveVersion);
        if (currentTable != null) {
            return currentTable;
        }
        return null;
    }

    @Override
    protected InputStream getFileInputStream() throws Exception {
        String customerSpace = MultiTenantContext.getShortTenantId();
        log.info("customer space is " + customerSpace);
        Table table = getCurrentConsolidateProductTable(MultiTenantContext.getShortTenantId());
        if (table == null) {
            log.error("no batch store table " + customerSpace);
            return new ByteArrayInputStream(StringUtils.EMPTY.getBytes(StandardCharsets.UTF_8));
        }
        List<Extract> extracts = table.getExtracts();
        HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder();

        // generate temporary csv file, transform avro to csv, return csv stream
        tempFileName = tempFolderName();
        log.info("temp file for csv " + tempFileName);
        CSVFormat format = LECSVFormat.format.withHeader(HEADERS);
        try (CSVPrinter csvFilePrinter = new CSVPrinter(new FileWriter(tempFileName), format)) {
            if (CollectionUtils.isNotEmpty(extracts)) {
                for (Extract extract : extracts) {
                    if (StringUtils.isNotBlank(extract.getPath())) {
                        String hdfsPath = pathBuilder.getFullPath(extract.getPath());
                        String s3Path = pathBuilder.convertAtlasTableDir(hdfsPath, podId, customerSpace, bucketName);
                        List<String> filePaths =  HdfsUtils.getFilesByGlobWithScheme(configuration, s3Path + "/*.avro", true);
                        if (CollectionUtils.isEmpty(filePaths)) {
                            continue;
                        }
                        for (String filePath : filePaths) {
                            log.info("s3 path is " + filePath);
                            try (FileReader<GenericRecord> reader = AvroUtils.getAvroFileReader(configuration,
                                    new Path(filePath))) {
                                for (GenericRecord record : reader) {
                                    String productType = getString(record, InterfaceName.ProductType.name());
                                    if (ProductType.Bundle.name().equals(productType)) {
                                        String id = getString(record, InterfaceName.ProductId.name());
                                        String bundle = getString(record, InterfaceName.ProductBundle.name());
                                        String productName = getString(record, InterfaceName.ProductName.name());
                                        String description = getString(record, InterfaceName.Description.name());
                                        csvFilePrinter.printRecord(id, productName, bundle, description);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        try {
            return new FileInputStream(tempFileName);
        } catch (FileNotFoundException exc) {
            log.error(String.format("Error while reading temporary file %s.", tempFileName), exc);
            return new ByteArrayInputStream(StringUtils.EMPTY.getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void downloadFile(HttpServletRequest request, HttpServletResponse response) {
        super.downloadFile(request, response);
        deleteFile(tempFileName);
    }

    private static String getString(GenericRecord record, String field) {
        String value;
        try {
            value = record.get(field).toString();
        } catch (Exception e) {
            value = "";
        }
        return value;
    }

    public static class BundleFileHttpDownloaderBuilder {
        private String mimeType;
        private String fileName;
        private String bucketName;
        private ImportFromS3Service importFromS3Service;
        private DataCollectionProxy dataCollectionProxy;
        private String podId;
        private Configuration configuration;

        public BundleFileHttpDownloaderBuilder setMimeType(String mimeType) {
            this.mimeType = mimeType;
            return this;
        }

        public BundleFileHttpDownloaderBuilder setFileName(String fileName) {
            this.fileName = fileName;
            return this;
        }

        public BundleFileHttpDownloaderBuilder setBucketName(String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        public BundleFileHttpDownloaderBuilder setImportFromS3Service(ImportFromS3Service importFromS3Service) {
            this.importFromS3Service = importFromS3Service;
            return this;
        }

        public BundleFileHttpDownloaderBuilder setDataCollectionProxy(DataCollectionProxy dataCollectionProxy) {
            this.dataCollectionProxy = dataCollectionProxy;
            return this;
        }

        public BundleFileHttpDownloaderBuilder setPodId(String podId) {
            this.podId = podId;
            return this;
        }

        public BundleFileHttpDownloaderBuilder setConfiguration(Configuration configuration) {
            this.configuration = configuration;
            return this;
        }
    }
}
