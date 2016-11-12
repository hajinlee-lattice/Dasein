package com.latticeengines.common.exposed.util;

import org.apache.http.client.HttpClient;
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

public class HttpClientUtils {

    private static final HttpComponentsClientHttpRequestFactory sslBlindHttpRequestFactory = sslBlindHttpRequestFactory();

    /**
     * gives a rest template using connection pool and ignore ssl.
     */
    public static RestTemplate newRestTemplate() {
        return new RestTemplate(sslBlindHttpRequestFactory);
    }

    /**
     * gives a blocking http client using connection pool and ignor ssl.
     */
    static HttpClient newHttpClient() {
        return HttpClientBuilder.create() //
                .setConnectionManager(constructTrustEveryThingConnectionMgr(SSLUtils.newSslBlindSocketFactory())) //
                .build();
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
