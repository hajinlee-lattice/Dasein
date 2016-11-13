package com.latticeengines.common.exposed.util;

import org.apache.http.client.HttpClient;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

public class HttpClientUtils {

    private static final PoolingHttpClientConnectionManager SSL_BLIND_CONNECTION_MGR = constructPoolingConnectionMgr(
            SSLUtils.SSL_BLIND_SOCKET_FACTORY);
    private static final HttpComponentsClientHttpRequestFactory SSL_BLIND_HC_FACTORY = constructHttpRequestFactory(
            SSL_BLIND_CONNECTION_MGR);
    private static final HttpComponentsClientHttpRequestFactory SSL_ENFORCED_HC_FACTORY = constructHttpRequestFactory(
            constructPoolingConnectionMgr(SSLConnectionSocketFactory.getSocketFactory()));

    /**
     * gives a rest template using connection pool and IGNORE ssl name
     * verification.
     */
    public static RestTemplate newRestTemplate() {
        return new RestTemplate(SSL_BLIND_HC_FACTORY);
    }

    /**
     * gives a rest template using connection pool and ENFORCE ssl name
     * verification.
     */
    public static RestTemplate newSSLEnforcedRestTemplate() {
        return new RestTemplate(SSL_ENFORCED_HC_FACTORY);
    }

    /**
     * gives a blocking http client using connection pool and ignor ssl.
     */
    static HttpClient newHttpClient() {
        return HttpClientBuilder.create() //
                .setConnectionManager(SSL_BLIND_CONNECTION_MGR) //
                .build();
    }

    private static PoolingHttpClientConnectionManager constructPoolingConnectionMgr(
            SSLConnectionSocketFactory sslSocketFactory) {
        Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory> create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory()) //
                .register("https", sslSocketFactory) //
                .build();
        PoolingHttpClientConnectionManager pool = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
        pool.setDefaultMaxPerRoute(16);
        pool.setMaxTotal(1024);
        return pool;
    }

    private static HttpComponentsClientHttpRequestFactory constructHttpRequestFactory(
            HttpClientConnectionManager connectionManager) {
        return new HttpComponentsClientHttpRequestFactory( //
                HttpClientBuilder.create() //
                        .setConnectionManager(connectionManager) //
                        .build());
    }

}
