package com.latticeengines.perf.cli.setup;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.ParseException;

import com.latticeengines.perf.job.properties.CommandLineProperties;

@SuppressWarnings("deprecation")
public abstract class CommandLineSetup<T> implements CommandLineProperties {

    protected CommandLineParser clp = new GnuParser();

    protected String customer;

    protected String restEndpointHost;

    public abstract void setupOptions(String[] args) throws ParseException;

    public abstract CommandLine getCommandLine();

    public abstract Class<T> getJobClassType();

    public abstract void setConfiguration(T t) throws Exception;

    public CommandLineSetup(String restEndpointHost) {
        this.restEndpointHost = restEndpointHost;
    }

    public void setCustomer(String customer) {
        this.customer = customer;
    }
}
