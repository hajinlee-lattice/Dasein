package org.apache.sqoop;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.cli.ToolOptions;
import com.cloudera.sqoop.tool.SqoopTool;
import com.cloudera.sqoop.util.OptionsFileUtil;

@SuppressWarnings("deprecation")
public class LedpSqoop extends Sqoop {


    private SqoopTool tool;
    private SqoopOptions options;
    private String[] childPrgmArgs;

    public LedpSqoop(SqoopTool tool, Configuration conf) {
        this(tool, conf, new SqoopOptions());
    }

    public LedpSqoop(SqoopTool tool, Configuration conf, SqoopOptions opts) {
        super(tool, conf, opts);
        this.options = opts;
        this.options.setConf(getConf());
        this.tool = tool;
    }

    /**
     * Entry-point that parses the correct SqoopTool to use from the args, but
     * does not call System.exit() as main() will.
     */
    public static int runTool(String[] args, Configuration conf) {
        // Expand the options
        String[] expandedArgs = null;
        try {
            expandedArgs = OptionsFileUtil.expandArguments(args);
        } catch (Exception ex) {
            LOG.error("Error while expanding arguments", ex);
            System.err.println(ex.getMessage());
            System.err.println("Try 'sqoop help' for usage.");
            return 1;
        }

        String toolName = expandedArgs[0];
        Configuration pluginConf = SqoopTool.loadPlugins(conf);
        SqoopTool tool = SqoopTool.getTool(toolName);
        if (null == tool) {
            System.err.println("No such sqoop tool: " + toolName + ". See 'sqoop help'.");
            return 1;
        }

        LedpSqoop sqoop = new LedpSqoop(tool, pluginConf);
        return sqoop.run(Arrays.copyOfRange(expandedArgs, 1, expandedArgs.length));
    }

    public int run(String[] args) {
        synchronized (LedpSqoop.class) {
            String[] toolArgs = stashChildPrgmArgs(args);
            Configuration conf = getConf();
            if (conf == null) {
                conf = new Configuration();
            }
            GenericOptionsParser parser = null;
            try {
                parser = new GenericOptionsParser(conf, toolArgs);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                LOG.debug(e.getMessage(), e);
            }

            setConf(conf);

            // get the args w/o generic hadoop args
            toolArgs = parser.getRemainingArgs();
            if (options.getConf() == null) {
                // Configuration wasn't initialized until after the ToolRunner
                // got us to this point. ToolRunner gave Sqoop itself a Conf
                // though.
                options.setConf(conf);
            }
            try {

                options = tool.parseArguments(toolArgs, null, options, false);
                tool.appendArgs(this.childPrgmArgs);
                tool.validateOptions(options);

                LOG.info("SqoopTool Run Options: " + options.toString());
                try {
                    LOG.info("SqoopTool Options: " + LedpSqoop.class.getClassLoader().getResource("org/apache/sqoop/LedpSqoop.class"));
                } catch (Exception e) {
                    LOG.info("Could not get location for Sqoop jar" + e.getMessage());
                }

                return tool.run(options);
            } catch (Exception e) {
                // Couldn't parse arguments.
                // Log the stack trace for this exception
                LOG.error(e.getMessage(), e);
                // Print exception message.
                System.err.println(e.getMessage());
                // Print the tool usage message and exit.
                ToolOptions toolOpts = new ToolOptions();
                tool.configureOptions(toolOpts);
                tool.printHelp(toolOpts);
                return 1; // Exit on exception here.
            }
        }
    }

    private String[] stashChildPrgmArgs(String[] argv) {
        for (int i = 0; i < argv.length; i++) {
            if ("--".equals(argv[i])) {
                this.childPrgmArgs = Arrays.copyOfRange(argv, i, argv.length);
                return Arrays.copyOfRange(argv, 0, i);
            }
        }

        // Didn't find child-program arguments.
        return argv;
    }

}
