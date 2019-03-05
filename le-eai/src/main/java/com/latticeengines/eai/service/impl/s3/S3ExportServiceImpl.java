package com.latticeengines.eai.service.impl.s3;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportContext;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.eai.HdfsToS3Configuration;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.ExtractUtils;
import com.latticeengines.eai.service.EaiYarnService;
import com.latticeengines.eai.service.ExportService;

@Component("s3ExportService")
public class S3ExportServiceImpl extends ExportService {

    private static final Logger log = LoggerFactory.getLogger(S3ExportServiceImpl.class);

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private EaiYarnService eaiYarnService;

    @Value("${aws.s3.bucket}")
    private String bucket;

    protected S3ExportServiceImpl() {
        super(ExportDestination.S3);
    }

    @Override
    public void exportDataFromHdfs(ExportConfiguration exportConfig, ExportContext context) {
        String prefix = ((HdfsToS3Configuration) exportConfig).getS3Prefix();
        String inputPath = exportConfig.getExportInputPath();
        String fileName = ((HdfsToS3Configuration) exportConfig).getTargetFilename();

        Table table = exportConfig.getTable();
        if (StringUtils.isEmpty(inputPath)) {
            exportConfig.setExportInputPath(ExtractUtils.getSingleExtractPath(yarnConfiguration, table));
        }

        log.info("parsed prefix : " + prefix);
        log.info("parsed fileName : " + fileName);
        log.info("parsed hdfsPath : " + inputPath);

        if (StringUtils.isEmpty(prefix) || "/".endsWith(prefix)) {
            throw new IllegalArgumentException("prefix cannot be null or /");
        }

        if (StringUtils.isEmpty(fileName)) {
            throw new IllegalArgumentException("fileName cannot be null");
        }

        if (StringUtils.isEmpty(inputPath)) {
            throw new IllegalArgumentException("hdfsPath cannot be null");
        }

        if (ExportFormat.AVRO.equals(exportConfig.getExportFormat())
                && ((HdfsToS3Configuration) exportConfig).getSplitSize() == null) {
            ((HdfsToS3Configuration) exportConfig).setSplitSize(100L * 1024 * 1024);
        }

        if (StringUtils.isEmpty(((HdfsToS3Configuration) exportConfig).getS3Bucket())) {
            ((HdfsToS3Configuration) exportConfig).setS3Bucket(bucket);
        }
        ApplicationId applicationId = eaiYarnService.submitSingleYarnContainerJob(exportConfig);
        context.setProperty(ExportProperty.APPID, applicationId);
    }

}
