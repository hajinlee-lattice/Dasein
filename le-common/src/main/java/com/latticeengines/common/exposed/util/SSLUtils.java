package com.latticeengines.common.exposed.util;

import java.security.cert.X509Certificate;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSLUtils {

    private static final Logger log = LoggerFactory.getLogger(SSLUtils.class);
    private static ThreadLocal<Boolean> sslOff = new ThreadLocal<>();

    public static final SSLConnectionSocketFactory SSL_BLIND_SOCKET_FACTORY = newSslBlindSocketFactory();
    private static final HostnameVerifier HOST_NAME_VERIFIER = (hostname, session) -> true;
    private static final TrustManager[] UNQUESTIONING_TRUST_MANAGER = new TrustManager[] { new X509TrustManager() {
        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
            return null;
        }

        public void checkClientTrusted(X509Certificate[] certs, String authType) {
        }

        public void checkServerTrusted(X509Certificate[] certs, String authType) {
        }
    } };

    public static void turnOffSSLNameVerification() {
        if (sslOff.get() == null || !sslOff.get()) {
            try {
                final SSLContext sc = SSLContext.getInstance("SSL");
                sc.init(null, UNQUESTIONING_TRUST_MANAGER, null);
                HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
                HttpsURLConnection.setDefaultHostnameVerifier(HOST_NAME_VERIFIER);
                sslOff.set(true);
                log.info("Turned off ssl for current thread: " + Thread.currentThread().getName());
            } catch (Exception e) {
                log.warn("Failed to turn off ssl for thread" + Thread.currentThread().getName());
            }
        }
    }

    private static SSLConnectionSocketFactory newSslBlindSocketFactory() {
        try {
            final SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, UNQUESTIONING_TRUST_MANAGER, null);
            return new SSLConnectionSocketFactory(sc, HOST_NAME_VERIFIER);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create a trust-everything connection manager ", e);
        }
    }

}