package com.latticeengines.dataflowapi.util;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;

// TODO Eventually move to a central location
@Component
public class MetadataProxy {

    @Value("${metadata.api.hostport}")
    private String hostname;

    public Table getMetadata(CustomerSpace space, String name) {
        String url = String.format("%s/metadata/customerspaces/%s/tables/%s", hostname, space, name);
        try {
            RestTemplate restTemplate = new RestTemplate();
            List<ClientHttpRequestInterceptor> interceptors = new ArrayList<>();
            interceptors.add(new MagicAuthenticationHeaderHttpRequestInterceptor());
            restTemplate.setInterceptors(interceptors);
            Table table = restTemplate.getForObject(url, Table.class);
            return table;
        } catch (Exception e) {
            throw new RuntimeException(String.format( //
                    "Failure retrieving metadata for table %s at address %s", name, url), e);
        }
    }

    public void setMetadata(CustomerSpace space, Table table) {
        String url = String.format("%s/metadata/customerspaces/%s/tables/%s", hostname, space, table.getName());
        try {
            RestTemplate restTemplate = new RestTemplate();
            List<ClientHttpRequestInterceptor> interceptors = new ArrayList<>();
            interceptors.add(new MagicAuthenticationHeaderHttpRequestInterceptor());
            restTemplate.setInterceptors(interceptors);
            restTemplate.postForLocation(url, table);
        } catch (Exception e) {
            throw new RuntimeException(String.format( //
                    "Failure setting metadata for table %s at address %s", table.getName(), url), e);
        }
    }
}
