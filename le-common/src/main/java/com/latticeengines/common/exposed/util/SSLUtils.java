package com.latticeengines.common.exposed.util;

import java.security.cert.X509Certificate;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

public class SSLUtils {
    private static Log log = LogFactory.getLog(SSLUtils.class);

    private static ThreadLocal<Boolean> sslOff = new ThreadLocal<>();

    /**
     * turn off ssl checking for all https request from now on in current thread
     */
    public static void turnOffSslChecking() {
        if (sslOff.get() == null || !sslOff.get()) {
            try {
                final TrustManager[] UNQUESTIONING_TRUST_MANAGER = new TrustManager[]{new X509TrustManager() {
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }

                    public void checkClientTrusted(X509Certificate[] certs, String authType) {
                    }

                    public void checkServerTrusted(X509Certificate[] certs, String authType) {
                    }
                }};
                final SSLContext sc = SSLContext.getInstance("SSL");
                sc.init(null, UNQUESTIONING_TRUST_MANAGER, null);
                HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
                HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
                    public boolean verify(String hostname, SSLSession session) {
                        return true;
                    }
                });
                sslOff.set(true);
                log.info("Turned off ssl for current thread: " + Thread.currentThread().getName());
            } catch (Exception e) {
                log.warn("Failed to turn off ssl for thread" + Thread.currentThread().getName(), e);
            }
        }
    }

    public static PoolingHttpClientConnectionManager getTrustEveryThingConnectionMgr() {
        try {
            HttpClientBuilder b = HttpClientBuilder.create();

            final TrustManager[] UNQUESTIONING_TRUST_MANAGER = new TrustManager[]{new X509TrustManager() {
                public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                    return null;
                }

                public void checkClientTrusted(X509Certificate[] certs, String authType) {
                }

                public void checkServerTrusted(X509Certificate[] certs, String authType) {
                }
            }};
            final SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, UNQUESTIONING_TRUST_MANAGER, null);

            final HostnameVerifier HOST_NAME_VERIFIER = new HostnameVerifier() {
                public boolean verify(String hostname, SSLSession session) {
                    return true;
                }
            };

            SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sc, HOST_NAME_VERIFIER);
            Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("http", PlainConnectionSocketFactory.getSocketFactory())
                    .register("https", sslSocketFactory)
                    .build();

            return new PoolingHttpClientConnectionManager( socketFactoryRegistry);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create a trust-everything connection manager ", e);
        }

    }

}
