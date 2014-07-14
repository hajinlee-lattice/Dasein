package com.latticeengines.perf.cli.setup.factory;

import org.apache.commons.cli.ParseException;

import com.latticeengines.perf.cli.setup.CommandLineSetup;
import com.latticeengines.perf.cli.setup.impl.CommandLineCreateSamplesSetup;
import com.latticeengines.perf.cli.setup.impl.CommandLineLoadDataSetup;
import com.latticeengines.perf.cli.setup.impl.CommandLineProfileSetup;
import com.latticeengines.perf.cli.setup.impl.CommandLineSubmitModelSetup;
import com.latticeengines.perf.job.properties.CommandLineProperties;

public class CommandLineSetupFactory {

    private static CommandLineSetup cls;

    public static CommandLineSetup create(String[] command) throws ParseException {
        String operation = command[1];
        String restEndpointHost = command[2];
        switch (operation.toLowerCase()) {
        case CommandLineProperties.OPERATION_LOAD:
            cls = new CommandLineLoadDataSetup(restEndpointHost);
            break;
        case CommandLineProperties.OPERATION_CREATE_SAMPLES:
            cls = new CommandLineCreateSamplesSetup(restEndpointHost);
            break;
        case CommandLineProperties.OPERATION_PROFILE:
            cls = new CommandLineProfileSetup(restEndpointHost);
            break;
        case CommandLineProperties.OPERATION_SUBMIT_MODEL:
            cls = new CommandLineSubmitModelSetup(restEndpointHost);
            break;
        }
        cls.setupOptions(command);
        return cls;
    }
}
