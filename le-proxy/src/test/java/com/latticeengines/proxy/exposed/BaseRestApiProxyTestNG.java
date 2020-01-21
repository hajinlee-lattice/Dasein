package com.latticeengines.proxy.exposed;

import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.util.SocketUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.mchange.v2.resourcepool.TimeoutException;

import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

@ContextConfiguration(locations = { "classpath:test-proxy-context.xml" })
public class BaseRestApiProxyTestNG extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(BaseRestApiProxyTestNG.class);

    @Inject
    private TestProxy testProxy;

    @Value("${proxy.test.keystore.path}")
    private String keystorePath;

    @Value("${proxy.test.keystore.password.encrypted}")
    private String keyStorePassword;

    private Undertow server;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        int port = SocketUtils.findAvailableTcpPort();
        log.info("Using local port " + port);
        server = getHttpServer(port);
        server.start();
        testProxy.setHostport("https://localhost:" + port);
    }

    @AfterClass(groups = "functional")
    public void teardown() {
        server.stop();
    }

    @Test(groups = "functional")
    public void testUrlExpansion() {
        testProxy.testUrlExpansion();
    }

    @Test(groups = "functional")
    public void testSuccessRetry() {
        Assert.assertEquals(testProxy.getRetry(), "Hello World");
    }

    @Test(groups = "functional")
    public void testSuccessRetryMono() {
        String content = testProxy.getRetryMono().block();
        Assert.assertEquals(content, "Hello World");
    }

    @Test(groups = "functional", dataProvider = "endpointProvider")
    public void testRetryAndFail(String endPoint, int expectedAttempts) {
        boolean thrown = false;
        AtomicInteger counter = new AtomicInteger();
        try {
            testProxy.getWithCounter(endPoint, counter);
        } catch (Exception e) {
            thrown = true;
        }
        assertTrue(thrown);
        Assert.assertEquals(counter.get(), expectedAttempts);
    }

    @Test(groups = "functional", dataProvider = "endpointProvider")
    public void testRetryMonoAndFail(String endPoint, int expectedAttempts) {
        boolean thrown = false;
        try {
            testProxy.getMonoAtEndpoint(endPoint).block();
        } catch (Exception e) {
            log.info(ExceptionUtils.getStackTrace(e));
            thrown = true;
        }
        assertTrue(thrown);
    }

    @DataProvider(name = "endpointProvider")
    public Object[][] provideEndpoints() {
        return new Object[][]{ //
                { "runtime", 1 }, //
                { "timeout", 5 }, //
                { "gateway", 5 } //
        };
    }

    private Undertow getHttpServer(int port) throws Exception {
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

        HttpHandler gatewayHdlr = new ErrorHandler(exchange -> {
            throw new RuntimeException("Gateway Error!");
        });

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(getKeyManagers(), null, null);

        return Undertow.builder()
                .addHttpsListener(port, "localhost", sslContext)
                .setHandler(Handlers.path() //
                        .addExactPath("/foo/baz/retry", retryHdlr) //
                        .addExactPath("/foo/baz/runtime", runtimeHdlr) //
                        .addExactPath("/foo/baz/timeout", timeoutHdlr) //
                        .addExactPath("/foo/baz/gateway", gatewayHdlr) //
                ).build();
    }

    private KeyManager[] getKeyManagers() {
        try {
            File keyStoreFile = new File(keystorePath);
            if (keyStoreFile.exists()) {
                KeyStore keyStore = KeyStore.getInstance("JKS");
                keyStore.load(new FileInputStream(keyStoreFile), keyStorePassword.toCharArray());
                KeyManagerFactory keyManagerFactory = KeyManagerFactory
                        .getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(keyStore, keyStorePassword.toCharArray());
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
        public void handleRequest(final HttpServerExchange exchange) {
            try {
                next.handleRequest(exchange);
            } catch (Exception e) {
                if(exchange.isResponseChannelAvailable()) {
                    exchange.setStatusCode(500);
                    if (e.getMessage().contains("Gateway")) {
                        exchange.setStatusCode(503);
                    }
                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                    exchange.getResponseSender().send(ExceptionUtils.getStackTrace(e));
                }
            }
        }
    }

}
