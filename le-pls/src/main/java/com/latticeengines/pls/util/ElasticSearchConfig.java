package com.latticeengines.pls.util;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class ElasticSearchConfig {

    @Value("${cdl.elasticsearch.http.scheme}")
    private String esScheme;
    @Value("${cdl.elasticsearch.host}")
    private String esHost;
    @Value("${cdl.elasticsearch.ports}")
    private String esPorts;
    @Value("${cdl.elasticsearch.user}")
    private String user;
    @Value("${cdl.elasticsearch.pwd.encrypted}")
    private String password;


    private static final int CONNECT_TIMEOUT = 1000;
    private static final int SOCKET_TIMEOUT = 30000;
    private static final int CONNECTION_REQUEST_TIMEOUT = 500;
    private static final int MAX_CONNECT_NUM = 100;
    private static final int MAX_CONNECT_PER_ROUTE = 100;

    @Bean
    public RestHighLevelClient restClient() {
        RestClientBuilder clientBuilder = RestClient.builder(new HttpHost(esHost, Integer.parseInt(esPorts), esScheme));
        //basic authentication mechanism
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));

        // Asynchronous httpclient connection number configuration and password configuration
        clientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
            httpClientBuilder.setMaxConnTotal(MAX_CONNECT_NUM);
            httpClientBuilder.setMaxConnPerRoute(MAX_CONNECT_PER_ROUTE);
            //Disable preemptive authentication
            httpClientBuilder.disableAuthCaching();
            return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        });
        //Asynchronous httpclient connection delay configuration
        clientBuilder.setRequestConfigCallback(builder -> {
            builder.setConnectTimeout(CONNECT_TIMEOUT);
            builder.setSocketTimeout(SOCKET_TIMEOUT);
            builder.setConnectionRequestTimeout(CONNECTION_REQUEST_TIMEOUT);
            return builder;
        });
        return new RestHighLevelClient(clientBuilder);


    }

    public RestHighLevelClient restClientDefault() {
        return new RestHighLevelClient(
                RestClient.builder(new HttpHost(esHost, Integer.parseInt(esPorts), esScheme)));
    }
}
