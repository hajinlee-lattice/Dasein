package com.latticeengines.perf.cli.setup;

import org.apache.commons.cli.Option;

public class CommandLineOption extends Option {
    public CommandLineOption(String opt, String longOpt, boolean hasArg, boolean required, String description) {
        super(opt, longOpt, hasArg, description);
        setRequired(required);
    }
}
