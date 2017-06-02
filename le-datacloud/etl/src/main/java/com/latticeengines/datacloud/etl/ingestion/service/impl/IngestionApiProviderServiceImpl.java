package com.latticeengines.datacloud.etl.ingestion.service.impl;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import javax.annotation.PostConstruct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionApiProviderService;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProviderService;
import com.latticeengines.domain.exposed.datacloud.ingestion.ApiConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.proxy.exposed.RestApiClient;

@Component("ingestionApiProviderService")
public class IngestionApiProviderServiceImpl implements IngestionProviderService, IngestionApiProviderService {
    @Autowired
    private ApplicationContext applicationContext;

    private RestApiClient apiClient;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    protected Configuration yarnConfiguration;

    @PostConstruct
    public void init() {
        apiClient = RestApiClient.newExternalClient(applicationContext);
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
            } else {

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
        DateFormat df = new SimpleDateFormat(config.getVersionFormat());
        TimeZone timezone = TimeZone.getTimeZone("UTC");
        df.setTimeZone(timezone);
        try {
            return HdfsPathBuilder.dateFormat.format(df.parse(version));
        } catch (ParseException e) {
            throw new RuntimeException(String.format("Failed to parse timestamp %s", version), e);
        }
    }
}
