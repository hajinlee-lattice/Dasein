package com.latticeengines.skald;

import java.util.Properties;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;

import org.python.util.PythonInterpreter;
import org.springframework.web.servlet.support.AbstractAnnotationConfigDispatcherServletInitializer;

public class SkaldInitializer extends AbstractAnnotationConfigDispatcherServletInitializer {
    @Override
    public void onStartup(ServletContext container) throws ServletException {
        super.onStartup(container);

        // Initialize the python interpreter.
        Properties properties = new Properties(System.getProperties());
        properties.setProperty("python.cachedir.skip", "true");
        PythonInterpreter.initialize(System.getProperties(), properties, new String[0]);
    }

    @Override
    protected Class<?>[] getRootConfigClasses() {
        return null;
    }

    @Override
    protected Class<?>[] getServletConfigClasses() {
        return new Class<?>[] { SkaldConfiguration.class };
    }

    @Override
    protected String[] getServletMappings() {
        return new String[] { "/" };
    }
}
