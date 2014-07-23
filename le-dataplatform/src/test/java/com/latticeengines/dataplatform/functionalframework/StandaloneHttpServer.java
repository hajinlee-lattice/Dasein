package com.latticeengines.dataplatform.functionalframework;

import javax.servlet.http.HttpServlet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

public class StandaloneHttpServer {

    private static final Log log = LogFactory.getLog(StandaloneHttpServer.class);
    private Server server;
    private Context root;

    public static final Integer SERVER_PORT = 8082;

    public void init() throws Exception {
        if (server == null) {
            server = new Server(SERVER_PORT);
            root = new Context(server, "/", Context.SESSIONS);
        }
    }

    public void start() throws Exception {
        log.info("Starting http server at port: " + SERVER_PORT);
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