package com.latticeengines.eai.service.impl.s3;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportContext;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.route.HdfsToS3Configuration;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.ExtractUtils;
import com.latticeengines.eai.exposed.service.EaiService;
import com.latticeengines.eai.service.ExportService;

@Component("s3ExportService")
public class S3ExportServiceImpl extends ExportService {

    private static final Log log = LogFactory.getLog(S3ExportServiceImpl.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private EaiService eaiService;

    @Value("${aws.s3.bucket}")
    private String bucket;

    protected S3ExportServiceImpl() {
        super(ExportDestination.S3);
    }

    @Override
    public void exportDataFromHdfs(ExportConfiguration exportConfig, ExportContext context) {
        if (StringUtils.isNotEmpty(exportConfig.getExportTargetPath())) {
            context.setProperty(ExportProperty.TARGETPATH, exportConfig.getExportTargetPath());
        } else {
            String targetPath = exportConfig.getProperties().get(ExportProperty.TARGET_DIRECTORY);
            context.setProperty(ExportProperty.TARGETPATH, targetPath);
        }

        context.setProperty(ExportProperty.TARGET_FILE_NAME,
                exportConfig.getProperties().get(ExportProperty.TARGET_FILE_NAME));

        context.setProperty(ExportProperty.CUSTOMER, exportConfig.getCustomerSpace().toString());
        Table table = exportConfig.getTable();

        String inputPath = exportConfig.getExportInputPath();
        if (StringUtils.isNotEmpty(inputPath)) {
            context.setProperty(ExportProperty.INPUT_FILE_PATH, inputPath);
        } else {
            context.setProperty(ExportProperty.INPUT_FILE_PATH,
                    ExtractUtils.getSingleExtractPath(yarnConfiguration, table));
        }

        ApplicationId applicationId = submitS3ExportJob(context, exportConfig.getExportFormat());
        context.setProperty(ImportProperty.APPID, applicationId);
    }

    @Override
    public ApplicationId submitDataExportJob(ExportConfiguration exportConfig) {
        ExportContext exportContext = new ExportContext(yarnConfiguration);
        exportDataFromHdfs(exportConfig, exportContext);
        return exportContext.getProperty(ImportProperty.APPID, ApplicationId.class);
    }

    private ApplicationId submitS3ExportJob(ExportContext context, ExportFormat format) {
        HdfsToS3Configuration routeConfiguration = getRouteConfiguration(context, format);
        ImportConfiguration importConfiguration = ImportConfiguration
                .createForAmazonS3Configuration(routeConfiguration);
        importConfiguration
                .setCustomerSpace(CustomerSpace.parse(context.getProperty(ExportProperty.CUSTOMER, String.class)));
        return eaiService.extractAndImport(importConfiguration);
    }

    private HdfsToS3Configuration getRouteConfiguration(ExportContext context, ExportFormat format) {
        String prefix = context.getProperty(ExportProperty.TARGETPATH, String.class);
        String hdfsPath = context.getProperty(ExportProperty.INPUT_FILE_PATH, String.class);
        String fileName = context.getProperty(ExportProperty.TARGET_FILE_NAME, String.class);

        log.info("parsed prefix : " + prefix);
        log.info("parsed fileName : " + fileName);
        log.info("parsed hdfsPath : " + hdfsPath);

        if (StringUtils.isEmpty(prefix) || "/".endsWith(prefix)) {
            throw new IllegalArgumentException("prefix cannot be null or /");
        }

        if (StringUtils.isEmpty(fileName)) {
            throw new IllegalArgumentException("fileName cannot be null");
        }

        if (StringUtils.isEmpty(hdfsPath)) {
            throw new IllegalArgumentException("hdfsPath cannot be null");
        }

        HdfsToS3Configuration configuration = new HdfsToS3Configuration();
        configuration.setS3Bucket(bucket);
        configuration.setS3Prefix(prefix);
        configuration.setHdfsPath(hdfsPath);
        configuration.setTargetFilename(fileName);
        if (ExportFormat.AVRO.equals(format)) {
            configuration.setSplitSize(100L * 1024 * 1024);
        }

        return configuration;
    }

}
