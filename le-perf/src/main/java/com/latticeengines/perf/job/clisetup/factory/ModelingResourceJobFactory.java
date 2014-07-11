package com.latticeengines.perf.job.clisetup.factory;

import org.apache.commons.cli.ParseException;

import com.latticeengines.perf.cli.setup.CommandLineSetup;
import com.latticeengines.perf.job.runnable.commandline.impl.CommandLineLoadData;
import com.latticeengines.perf.job.runnable.commandline.impl.CommandLineProfile;
import com.latticeengines.perf.job.runnable.commandline.impl.CommandLineSubmitModel;
import com.latticeengines.perf.job.runnable.commandline.impl.CommaneLineCreateSamples;

public class ModelingResourceJobFactory {

    private static CommandLineSetup cls;

    public static CommandLineSetup create(String[] command) throws ParseException {
        String operation = command[1];
        switch (operation.toLowerCase()) {
        case "load":
            cls = new CommandLineLoadData();
            break;
        case "createsamples":
            cls = new CommaneLineCreateSamples();
            break;
        case "profile":
            cls = new CommandLineProfile();
            break;
        case "submitmodel":
            cls = new CommandLineSubmitModel();
            break;
        }
        cls.setupOptions(command);
        return cls;
    }
}
