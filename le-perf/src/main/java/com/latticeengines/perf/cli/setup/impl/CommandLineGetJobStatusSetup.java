package com.latticeengines.perf.cli.setup.impl;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.latticeengines.perf.cli.setup.CommandLineOption;
import com.latticeengines.perf.cli.setup.CommandLineSetup;
import com.latticeengines.perf.job.runnable.impl.GetJobStatus;

public class CommandLineGetJobStatusSetup extends CommandLineSetup<GetJobStatus> {

    private CommandLine cl;

    public CommandLineGetJobStatusSetup(String restEndpointHost) {
        super(restEndpointHost);
    }

    @Override
    public void setupOptions(String[] args) throws ParseException {
        Options ops = new Options();
        Option appId = new CommandLineOption(APPLICATIONID_OPT, APPLICATIONID_LONGOPT, true, true, APPLICATIONID_DEF);
        ops.addOption(appId);
        cl = clp.parse(ops, args);
    }

    @Override
    public CommandLine getCommandLine() {
        return cl;
    }

    @Override
    public Class<GetJobStatus> getJobClassType() {
        return GetJobStatus.class;
    }

    public void setConfiguration(GetJobStatus gjs) throws Exception {
        String appId = cl.getOptionValue(APPLICATIONID_OPT);
        gjs.setConfiguration(restEndpointHost, appId);
    }
}
