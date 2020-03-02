package com.latticeengines.datacloud.etl.ingestion.service.impl;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionAPIProviderService;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProgressService;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionVersionService;
import com.latticeengines.domain.exposed.datacloud.ingestion.ApiConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.proxy.exposed.RestApiClient;

@Component("ingestionAPIProviderService")
public class IngestionAPIProviderServiceImpl extends IngestionProviderServiceImpl
        implements IngestionAPIProviderService {

    private static final Logger log = LoggerFactory.getLogger(IngestionAPIProviderServiceImpl.class);

    @Inject
    private IngestionProgressService ingestionProgressService;

    @Inject
    private IngestionVersionService ingestionVersionService;

    @Inject
    private ApplicationContext applicationContext;

    private RestApiClient apiClient;

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @Inject
    protected Configuration yarnConfiguration;

    @PostConstruct
    public void init() {
        apiClient = RestApiClient.newExternalClient(applicationContext);
    }

    @Override
    public void ingest(IngestionProgress progress) throws Exception {
        try {
            Path ingestionDir = new Path(progress.getDestination()).getParent();
            if (HdfsUtils.isDirectory(yarnConfiguration, ingestionDir.toString())) {
                HdfsUtils.rmdir(yarnConfiguration, ingestionDir.toString());
            }
            ApiConfiguration apiConfig = (ApiConfiguration) progress.getIngestion().getProviderConfiguration();
            log.info(String.format("Downloading from %s ...", apiConfig.getFileUrl()));
            injestCopy(progress, apiConfig);
            log.info("Download completed");
            Long size = HdfsUtils.getFileSize(yarnConfiguration, progress.getDestination());
            progress = ingestionProgressService.updateProgress(progress).size(size).status(ProgressStatus.FINISHED)
                    .commit(true);
            checkCompleteVersionFromApi(progress.getIngestion(), progress.getVersion());
        } catch (Exception e) {
            progress = ingestionProgressService.updateProgress(progress).status(ProgressStatus.FAILED).commit(true);
            log.error(String.format("Ingestion failed of exception %s. Progress: %s", e.getMessage(),
                    progress.toString()));
        }
    }

    public void injestCopy(IngestionProgress progress, ApiConfiguration apiConfig)
            throws MalformedURLException, IOException {
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod(apiConfig.getFileUrl());
        int statusCode = client.executeMethod(method);
        log.info("Starting to download. status=" + statusCode);
        InputStream in = new BufferedInputStream(method.getResponseBodyAsStream());
        FileSystem hdfs = FileSystem.get(yarnConfiguration);
        FSDataOutputStream outStream = hdfs.create(new Path(progress.getDestination()));
        long bytes = IOUtils.copy(in, outStream);
        log.info("Downlowned files size=" + bytes);
        in.close();
        outStream.close();
    }

    @Override
    @SuppressWarnings("static-access")
    public List<String> getMissingFiles(Ingestion ingestion) {
        List<String> result = new ArrayList<String>();
        ApiConfiguration apiConfiguration = (ApiConfiguration) ingestion.getProviderConfiguration();
        String targetVersion = getTargetVersion(apiConfiguration);
        com.latticeengines.domain.exposed.camille.Path ingestionDir = hdfsPathBuilder
                .constructIngestionDir(ingestion.getIngestionName(), targetVersion);
        Path success = new Path(ingestionDir.toString(), hdfsPathBuilder.SUCCESS_FILE);
        try {
            if (!HdfsUtils.isDirectory(yarnConfiguration, ingestionDir.toString())
                    || !HdfsUtils.fileExists(yarnConfiguration, success.toString())) {
                result.add(apiConfiguration.getFileName());
            }
        } catch (IOException e) {
            throw new RuntimeException("Fail to look for missing files for ingestion " + ingestion.toString(), e);
        }
        return result;
    }

    @Override
    public String getTargetVersion(ApiConfiguration config) {
        String version = null;
        try {
            version = apiClient.get(null, config.getVersionUrl());
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to call api %s to get version", config.getVersionUrl()),
                    e);
        }
        version = version.replace("\n", "").replace("\r", "");
        DateFormat df = new SimpleDateFormat(config.getVersionFormat());
        TimeZone timezone = TimeZone.getTimeZone("UTC");
        df.setTimeZone(timezone);
        try {
            return HdfsPathBuilder.dateFormat.format(df.parse(version));
        } catch (ParseException e) {
            throw new RuntimeException(String.format("Failed to parse timestamp %s", version), e);
        }
    }

    @SuppressWarnings("static-access")
    private void checkCompleteVersionFromApi(Ingestion ingestion, String version) {
        ApiConfiguration apiConfig = (ApiConfiguration) ingestion.getProviderConfiguration();
        com.latticeengines.domain.exposed.camille.Path hdfsDir = hdfsPathBuilder
                .constructIngestionDir(ingestion.getIngestionName(), version);
        Path success = new Path(hdfsDir.toString(), hdfsPathBuilder.SUCCESS_FILE);
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, success.toString())) {
                return;
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to check %s in HDFS dir %s", hdfsPathBuilder.SUCCESS_FILE,
                    hdfsDir.toString()), e);
        }
        Path file = new Path(hdfsDir.toString(), apiConfig.getFileName());
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, file.toString())) {
                HdfsUtils.writeToFile(yarnConfiguration, success.toString(), "");
                emailNotify(apiConfig, ingestion.getIngestionName(), version, hdfsDir.toString());
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to check %s in HDFS or create %s in HDFS dir %s",
                    file.toString(), hdfsPathBuilder.SUCCESS_FILE, hdfsDir.toString()), e);
        }
        ingestionVersionService.updateCurrentVersion(ingestion, version);
    }
}
