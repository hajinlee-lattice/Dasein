package com.latticeengines.common.exposed.util;

import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

public class RestTemplateUtils {

    private static final HttpComponentsClientHttpRequestFactory sslBlindHttpRequestFactory = sslBlindHttpRequestFactory();

    public static RestTemplate newSSLBlindRestTemplate() {
        return new RestTemplate(sslBlindHttpRequestFactory);
    }

    private static HttpComponentsClientHttpRequestFactory sslBlindHttpRequestFactory() {
        return constructHttpRequestFactory(constructTrustEveryThingConnectionMgr(SSLUtils.newSslBlindSocketFactory()));
    }

    private static PoolingHttpClientConnectionManager constructTrustEveryThingConnectionMgr(
            SSLConnectionSocketFactory sslSocketFactory) {
        Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory> create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory()) //
                .register("https", sslSocketFactory) //
                .build();
        return new PoolingHttpClientConnectionManager(socketFactoryRegistry);
    }

    private static HttpComponentsClientHttpRequestFactory constructHttpRequestFactory(
            HttpClientConnectionManager connectionManager) {
        CloseableHttpClient httpClient = HttpClientBuilder.create() //
                .setConnectionManager(connectionManager) //
                .build();
        return new HttpComponentsClientHttpRequestFactory(httpClient);
    }

}
