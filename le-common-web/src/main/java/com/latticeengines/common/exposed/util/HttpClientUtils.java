package com.latticeengines.common.exposed.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpHeaders;
import org.apache.http.client.HttpClient;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.client.reactive.ClientHttpConnector;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.converter.KryoHttpMessageConverter;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

public final class HttpClientUtils {

    protected HttpClientUtils() {
        throw new UnsupportedOperationException();
    }

    public static final PoolingHttpClientConnectionManager SSL_BLIND_CONNECTION_MGR = constructPoolingConnectionMgr(
            SSLUtils.SSL_BLIND_SOCKET_FACTORY);
    private static final HttpComponentsClientHttpRequestFactory SSL_BLIND_HC_FACTORY = constructHttpRequestFactory(
            SSL_BLIND_CONNECTION_MGR);
    private static final HttpComponentsClientHttpRequestFactory SSL_ENFORCED_HC_FACTORY = constructHttpRequestFactory(
            constructPoolingConnectionMgr(SSLConnectionSocketFactory.getSocketFactory()));
    private static final ClientHttpConnector SSL_BLINK_HTTP_CONNECTOR = SSLUtils.newSslBlindHttpConnector();

    /**
     * gives a reactive web client using connection pool and IGNORE ssl name
     * verification.
     */
    public static WebClient newWebClient() {
        return WebClient.builder().clientConnector(SSL_BLINK_HTTP_CONNECTOR).build();
    }

    public static WebClient newWebClient(@NotNull ExchangeFilterFunction fn) {
        Preconditions.checkNotNull(fn, "Exchange filter function should not be null");
        return WebClient.builder().clientConnector(SSL_BLINK_HTTP_CONNECTOR).filter(fn).build();
    }

    /**
     * gives a rest template using connection pool and IGNORE ssl name
     * verification.
     */
    public static RestTemplate newRestTemplate() {
        RestTemplate restTemplate = new RestTemplate(SSL_BLIND_HC_FACTORY);
        appendKryoMessageConverters(restTemplate);
        return restTemplate;
    }

    /**
     * generate a rest template using ssl enforced connection pool and process data in
     * JSON format.
     */
    public static RestTemplate newJsonRestTemplate() {
        RestTemplate template = newSSLEnforcedRestTemplate();
        appendJacksonMessageConverters(template);
        template.getInterceptors().addAll(jsonInterceptors());
        return template;
    }

    public static RestTemplate newFormURLEncodedRestTemplate() {
        RestTemplate restTemplate = new RestTemplate(SSL_BLIND_HC_FACTORY);
        List<ClientHttpRequestInterceptor> interceptors = new ArrayList<>();
        interceptors.add(
                new HeaderRequestInterceptor(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED_VALUE));
        interceptors.add(new HeaderRequestInterceptor(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE));
        restTemplate.setInterceptors(interceptors);
        return restTemplate;
    }

    /**
     * gives a rest template using connection pool and ENFORCE ssl name
     * verification.
     */
    public static RestTemplate newSSLEnforcedRestTemplate() {
        return new RestTemplate(SSL_ENFORCED_HC_FACTORY);
    }

    /**
     * gives a blocking http client using connection pool and ignore ssl.
     */
    static HttpClient newHttpClient() {
        return HttpClientBuilder.create() //
                .setConnectionManager(SSL_BLIND_CONNECTION_MGR) //
                .setDefaultHeaders(Arrays.asList( //
                        new BasicHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE), //
                        new BasicHeader(HttpHeaders.ACCEPT_ENCODING, "gzip") //
                ))//
                .build();
    }

    public static HttpComponentsClientHttpRequestFactory getSslBlindRequestFactory() {
        return SSL_BLIND_HC_FACTORY;
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
        //pool.setValidateAfterInactivity(2000); // 2 secs (default 5s)
        return pool;
    }

    private static HttpComponentsClientHttpRequestFactory constructHttpRequestFactory(
            HttpClientConnectionManager connectionManager) {
        HttpComponentsClientHttpRequestFactory reqFac = new HttpComponentsClientHttpRequestFactory( //
                HttpClientBuilder.create() //
                        .setConnectionManager(connectionManager) //
                        .evictExpiredConnections()
                        .evictIdleConnections(30L, TimeUnit.SECONDS)
                        .build());
        reqFac.setConnectionRequestTimeout(10000); // 10 sec
        reqFac.setConnectTimeout(600000); // 10 min
        reqFac.setReadTimeout(600000); // 10 min
        return reqFac;
    }

    @SuppressWarnings("unused")
    private static List<ClientHttpRequestInterceptor> defaultInterceptors() {
        List<ClientHttpRequestInterceptor> interceptors = new ArrayList<>();
        interceptors.add(new HeaderRequestInterceptor(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE));
        interceptors.add(new HeaderRequestInterceptor(HttpHeaders.ACCEPT_ENCODING, "gzip"));
        return interceptors;
    }

    private static List<ClientHttpRequestInterceptor> jsonInterceptors() {
        List<ClientHttpRequestInterceptor> interceptors = new ArrayList<>();
        interceptors.add(new HeaderRequestInterceptor(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE));
        interceptors.add(new HeaderRequestInterceptor(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE));
        interceptors.add(new HeaderRequestInterceptor(HttpHeaders.ACCEPT_ENCODING, "gzip"));
        return interceptors;
    }

    private static void appendKryoMessageConverters(RestTemplate restTemplate) {
        List<HttpMessageConverter<?>> newConverters = new ArrayList<>(restTemplate.getMessageConverters());
        newConverters.add(new KryoHttpMessageConverter());
        restTemplate.setMessageConverters(newConverters);
    }

    private static void appendJacksonMessageConverters(RestTemplate restTemplate) {
        MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
        converter.setSupportedMediaTypes(Collections.singletonList(MediaType.ALL));
        restTemplate.getMessageConverters().add(converter);
    }

    /**
     * This is command line tool to check https connection
     */
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: " + SSLUtils.class.getName() + " <url>");
            System.exit(1);
        }
        try {
            RestTemplate restTemplate = new RestTemplate();
            SSLUtils.turnOffSSLNameVerification();
            String content = restTemplate.getForObject(args[0], String.class);
            System.out.println(content.substring(0, Math.min(content.length(), 1000)));
            System.out.println("Successfully connected");

            SSLUtils.turnOnSSLNameVerification();
            try {
                content = restTemplate.getForObject(args[0], String.class);
            } catch (ResourceAccessException e) {
                System.out.println("Got ResourceAccessException as expected.");
            }

            SSLUtils.turnOffSSLNameVerification();
            content = restTemplate.getForObject(args[0], String.class);
            System.out.println(content.substring(0, Math.min(content.length(), 1000)));
            System.out.println("Successfully connected");

            restTemplate = HttpClientUtils.newRestTemplate();
            SSLUtils.turnOffSSLNameVerification();
            content = restTemplate.getForObject(args[0], String.class);
            System.out.println(content.substring(0, Math.min(content.length(), 1000)));
            System.out.println("Successfully connected");

        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

}
