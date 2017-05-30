package com.latticeengines.datacloud.etl.ingestion.service.impl;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionApiProviderService;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProviderService;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionVersionService;
import com.latticeengines.domain.exposed.datacloud.ingestion.ApiConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.proxy.exposed.RestApiClient;

@Component("ingestionApiProviderService")
public class IngestionApiProviderServiceImpl implements IngestionProviderService, IngestionApiProviderService {
    @Autowired
    private ApplicationContext applicationContext;

    private RestApiClient apiClient;

    @Autowired
    private IngestionVersionService ingestionVersionService;

    @PostConstruct
    public void init() {
        apiClient = RestApiClient.newExternalClient(applicationContext);
    }

    @Override
    public List<String> getMissingFiles(Ingestion ingestion) {
        List<String> result = new ArrayList<String>();
        ApiConfiguration apiConfiguration = (ApiConfiguration) ingestion.getProviderConfiguration();
        String targetVersion = getTargetVersion(apiConfiguration);
        List<String> existingVersions = ingestionVersionService
                .getMostRecentVersionsFromHdfs(ingestion.getIngestionName(), 1);
        Set<String> existingVersionsSet = new HashSet<String>(existingVersions);
        if (!existingVersionsSet.contains(targetVersion)) {
            result.add(apiConfiguration.getFileName());
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
