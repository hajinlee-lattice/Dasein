package com.latticeengines.proxy.exposed;

import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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

    @Autowired
    private TestProxy testProxy;

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
        Assert.assertEquals(testProxy.testRetry(), "Hello World");
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

    @DataProvider(name = "endpointProvider")
    public Object[][] provideEndpoints() {
        return new Object[][]{ //
                { "runtime", 1 }, //
                { "timeout", 5 }, //
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

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(getKeyManagers(), null, null);

        return Undertow.builder()
                .addHttpsListener(port, "localhost", sslContext)
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
