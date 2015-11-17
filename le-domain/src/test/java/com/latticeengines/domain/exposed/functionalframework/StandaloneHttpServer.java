package com.latticeengines.domain.exposed.functionalframework;

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

    private int port;
    public static final Integer SERVER_PORT = 8082;

    public void init() throws Exception {
        if (server == null) {
            this.port = SERVER_PORT;
            server = new Server(SERVER_PORT);
            root = new Context(server, "/", Context.SESSIONS);
        }
    }

    public void init(int port) throws Exception {
        if (server == null) {
            this.port = port;
            server = new Server(port);
            root = new Context(server, "/", Context.SESSIONS);
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