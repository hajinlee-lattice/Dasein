package com.latticeengines.skald;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.python.util.PythonInterpreter;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;

public class SkaldServer {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties(System.getProperties());
        properties.setProperty("python.cachedir.skip", "true");
        PythonInterpreter.initialize(System.getProperties(), properties, new String[0]);

        AnnotationConfigWebApplicationContext context = new AnnotationConfigWebApplicationContext();
        context.setConfigLocation("com.latticeengines.skald");

        ServletContextHandler handler = new ServletContextHandler();
        handler.setErrorHandler(null);
        handler.setContextPath(path);
        handler.addServlet(new ServletHolder(new DispatcherServlet(context)), "/");
        handler.addEventListener(new ContextLoaderListener(context));

        Server server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setHost("0.0.0.0");
        connector.setPort(port);
        server.addConnector(connector);
        server.setHandler(handler);

        server.start();
        log.info("Server running on port " + connector.getPort());
        server.join();
    }

    private static final int port = 8040;
    private static final String path = "/";

    private static final Log log = LogFactory.getLog(SkaldServer.class);
}
