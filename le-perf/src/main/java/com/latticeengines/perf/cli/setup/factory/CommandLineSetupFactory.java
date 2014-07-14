package com.latticeengines.perf.cli.setup.factory;

import org.apache.commons.cli.ParseException;

import com.latticeengines.perf.cli.setup.CommandLineSetup;
import com.latticeengines.perf.cli.setup.impl.CommandLineCreateSamplesSetup;
import com.latticeengines.perf.cli.setup.impl.CommandLineLoadDataSetup;
import com.latticeengines.perf.cli.setup.impl.CommandLineProfileSetup;
import com.latticeengines.perf.cli.setup.impl.CommandLineSubmitModelSetup;

public class CommandLineSetupFactory {

    private static CommandLineSetup cls;

    public static CommandLineSetup create(String[] command) throws ParseException {
        String operation = command[1];
        String restEndpointHost = command[2];
        switch (operation.toLowerCase()) {
        case "load":
            cls = new CommandLineLoadDataSetup(restEndpointHost);
            break;
        case "createsamples":
            cls = new CommandLineCreateSamplesSetup(restEndpointHost);
            break;
        case "profile":
            cls = new CommandLineProfileSetup(restEndpointHost);
            break;
        case "submitmodel":
            cls = new CommandLineSubmitModelSetup(restEndpointHost);
            break;
        }
        cls.setupOptions(command);
        return cls;
    }
}
