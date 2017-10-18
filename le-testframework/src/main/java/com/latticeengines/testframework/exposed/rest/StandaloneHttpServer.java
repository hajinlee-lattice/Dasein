package com.latticeengines.testframework.exposed.rest;

import javax.servlet.http.HttpServlet;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandaloneHttpServer {

    private static final Logger log = LoggerFactory.getLogger(StandaloneHttpServer.class);
    private Server server;
    private ServletContextHandler root;

    private int port;
    private static final Integer SERVER_PORT = 8082;

    public void init() throws Exception {
        init(SERVER_PORT);
    }

    public void init(int port) throws Exception {
        if (server == null) {
            this.port = port;
            server = new Server(port);
            root = new ServletContextHandler(server, "/", ServletContextHandler.SESSIONS);
        }
    }

    public void start() throws Exception {
        log.info("Starting http server at port: " + port);
        if (!server.isStarted()) {
            server.start();
            log.info("Server started");
        }
    }

    public void stop() throws Exception {
        server.stop();
    }

    public void addServlet(HttpServlet servlet, String url) {
        root.addServlet(new ServletHolder(servlet), url);
    }

}
