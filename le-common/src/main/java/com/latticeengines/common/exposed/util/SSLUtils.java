package com.latticeengines.common.exposed.util;

import java.io.InputStream;
import java.io.OutputStream;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSLUtils {

    private static final Logger log = LoggerFactory.getLogger(SSLUtils.class);
    private static ThreadLocal<Boolean> sslOff = new ThreadLocal<>();

    public static final SSLConnectionSocketFactory SSL_BLIND_SOCKET_FACTORY = newSslBlindSocketFactory();
    private static final HostnameVerifier DEFAULT_HOST_NAME_VERIFIER = HttpsURLConnection.getDefaultHostnameVerifier();
    private static final HostnameVerifier BLIND_HOST_NAME_VERIFIER = (hostname, session) -> true;

    public static void turnOffSSLNameVerification() {
        switchSSLNameVerification(false);
    }

    public static void turnOnSSLNameVerification() {
        switchSSLNameVerification(true);
    }

    private static void switchSSLNameVerification(boolean on) {
        if (sslOff.get() == null || !sslOff.get().equals(on)) {
            String action = on ? "on" : "off";
            try {
                HostnameVerifier verifier = on ? DEFAULT_HOST_NAME_VERIFIER : BLIND_HOST_NAME_VERIFIER;
                HttpsURLConnection.setDefaultHostnameVerifier(verifier);
                sslOff.set(on);
                log.info("Turned " + action + " ssl for current thread: " + Thread.currentThread().getName());
            } catch (Exception e) {
                log.warn("Failed to turn " + action + " ssl for thread" + Thread.currentThread().getName());
            }
        }
    }

    private static SSLConnectionSocketFactory newSslBlindSocketFactory() {
        try {
            SSLSocketFactory sf = (SSLSocketFactory) SSLSocketFactory.getDefault();
            return new SSLConnectionSocketFactory(sf, BLIND_HOST_NAME_VERIFIER);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create a trust-everything connection manager ", e);
        }
    }

    /**
     * This is command line tool to check ssl connection
     */
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: " + SSLUtils.class.getName() + " <host> <port>");
            System.exit(1);
        }
        try {
            SSLSocketFactory defaultSSLSocketFactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
            SSLSocket sslsocket = (SSLSocket) defaultSSLSocketFactory.createSocket(args[0], Integer.parseInt(args[1]));

            InputStream in = sslsocket.getInputStream();
            OutputStream out = sslsocket.getOutputStream();

            // Write a test byte to get a reaction :)
            out.write(1);

            while (in.available() > 0) {
                System.out.print(in.read());
            }
            System.out.println("Successfully connected");

        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

}
