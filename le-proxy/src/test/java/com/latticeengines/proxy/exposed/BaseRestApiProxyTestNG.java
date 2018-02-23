package com.latticeengines.proxy.exposed;

import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.client.ResourceAccessException;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.mchange.v2.resourcepool.TimeoutException;

import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

import static org.testng.Assert.assertTrue;

@ContextConfiguration(locations = { "classpath:test-proxy-context.xml" })
public class BaseRestApiProxyTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private TestProxy testProxy;

    @Test(groups = "functional")
    public void testUrlExpansion() {
        testProxy.testUrlExpansion();
    }

    @Test(groups = "functional")
    public void testRetry() throws Exception {
        Undertow server = getHttpServer();
        server.start();
        try {
            Assert.assertEquals(testProxy.testRetry(), "Hello World");

            boolean thrown = false;
            try {
                testProxy.testDirectlyFail();
            } catch (Exception e) {
                thrown = true;
            }
            assertTrue(thrown);

            thrown = false;
            try {
                testProxy.testRetryAndFail();
            } catch (Exception e) {
                thrown = true;
            }
            assertTrue(thrown);

        } finally {
            server.stop();
        }
    }

    private Undertow getHttpServer() throws Exception {
        HttpHandler retryHdlr = new ErrorHandler(new HttpHandler() {
            private AtomicLong counter = new AtomicLong();
            @Override
            public void handleRequest(HttpServerExchange exchange) throws Exception {
                if (counter.getAndIncrement() < 3) {
                    throw new TimeoutException("You need to retry more!");
                } else {
                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                    exchange.getResponseSender().send("Hello World");
                }
            }
        });

        HttpHandler runtimeHdlr = new ErrorHandler(exchange -> {
            throw new RuntimeException("This will fail!");
        });

        HttpHandler timeoutHdlr = new ErrorHandler(exchange -> {
            throw new TimeoutException("Jdbc connection timeout.");
        });

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(getKeyManagers(), null, null);

        return Undertow.builder()
                .addHttpListener(8084, "localhost")
                .addHttpsListener(9084, "localhost", sslContext)
                .setHandler(Handlers.path() //
                        .addExactPath("/foo/baz/retry", retryHdlr) //
                        .addExactPath("/foo/baz/runtime", runtimeHdlr) //
                        .addExactPath("/foo/baz/timeout", timeoutHdlr) //
                ).build();
    }

    private static KeyManager[] getKeyManagers() {
        try {
            File keyStoreFile = new File("/etc/ledp/tls/ledp_keystore.jks");
            if (keyStoreFile.exists()) {
                KeyStore keyStore = KeyStore.getInstance("JKS");
                keyStore.load(new FileInputStream(keyStoreFile), "Lattice1".toCharArray());
                KeyManagerFactory keyManagerFactory = KeyManagerFactory
                        .getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(keyStore, "Lattice1".toCharArray());
                return keyManagerFactory.getKeyManagers();
            } else {
                return null;
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private class ErrorHandler implements HttpHandler {

        private final HttpHandler next;

        private ErrorHandler(final HttpHandler next) {
            this.next = next;
        }

        @Override
        public void handleRequest(final HttpServerExchange exchange) throws Exception {
            try {
                next.handleRequest(exchange);
            } catch (Exception e) {
                if(exchange.isResponseChannelAvailable()) {
                    exchange.setStatusCode(500);
                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                    exchange.getResponseSender().send(ExceptionUtils.getStackTrace(e));
                }
            }
        }
    }

}
