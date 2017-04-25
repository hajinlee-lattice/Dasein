package com.latticeengines.datacloud.etl.ingestion.service.impl;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionApiProviderService;
import com.latticeengines.domain.exposed.datacloud.ingestion.ApiConfiguration;
import com.latticeengines.proxy.exposed.RestApiClient;

@Component("ingestionApiProviderService")
public class IngestionApiProviderServiceImpl implements IngestionApiProviderService {
    private RestApiClient apiClient = new RestApiClient();

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
