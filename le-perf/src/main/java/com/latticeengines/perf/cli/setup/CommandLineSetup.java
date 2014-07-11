package com.latticeengines.perf.cli.setup;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.ParseException;

public interface CommandLineSetup {

    public CommandLineParser clp = new GnuParser();

    public final char DELIMETER = ';';

    public void setupOptions(String[] args) throws ParseException;

    public CommandLine getCommandLine();
}
