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
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;

public class SSLUtils {

    private static final Log log = LogFactory.getLog(SSLUtils.class);
    private static ThreadLocal<Boolean> sslOff = new ThreadLocal<>();

    static final SSLConnectionSocketFactory SSL_BLIND_SOCKET_FACTORY = newSslBlindSocketFactory();

    public static void turnOffSSL() {
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

    private static SSLConnectionSocketFactory newSslBlindSocketFactory() {
        try {
            final TrustManager[] UNQUESTIONING_TRUST_MANAGER = new TrustManager[] { new X509TrustManager() {
                public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                    return null;
                }

                public void checkClientTrusted(X509Certificate[] certs, String authType) {
                }

                public void checkServerTrusted(X509Certificate[] certs, String authType) {
                }
            } };
            final SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, UNQUESTIONING_TRUST_MANAGER, null);

            final HostnameVerifier HOST_NAME_VERIFIER = new HostnameVerifier() {
                public boolean verify(String hostname, SSLSession session) {
                    return true;
                }
            };

            return new SSLConnectionSocketFactory(sc, HOST_NAME_VERIFIER);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create a trust-everything connection manager ", e);
        }
    }

}
