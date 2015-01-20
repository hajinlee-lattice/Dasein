package com.latticeengines.marketoadapter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.BasicConfigurator;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;

public class MarketoAdapterServer
{
	public static void main(String[] args) throws Exception
	{
	    // TODO: Add an actual logging configuration.
	    BasicConfigurator.configure();
	    
	    AnnotationConfigWebApplicationContext context = new AnnotationConfigWebApplicationContext();
	    context.setConfigLocation("com.latticeengines.skald");
	    
        ServletContextHandler handler = new ServletContextHandler();
        handler.setErrorHandler(null);
        handler.setContextPath("/");
        handler.addServlet(new ServletHolder(new DispatcherServlet(context)), "/");
        handler.addEventListener(new ContextLoaderListener(context));
	    
	    Server server = new Server();
		ServerConnector connector = new ServerConnector(server);
		connector.setHost("0.0.0.0");
		connector.setPort(8040);
		server.addConnector(connector);
        server.setHandler(handler);
        
		server.start();
        log.info("Server running on port " + connector.getPort());
		server.join();
	}

	private static final Log log = LogFactory.getLog(MarketoAdapterServer.class);
}
