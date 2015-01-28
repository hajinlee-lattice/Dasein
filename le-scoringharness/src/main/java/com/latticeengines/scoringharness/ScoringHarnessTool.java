package com.latticeengines.scoringharness;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.internal.HelpScreenException;

import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import ch.qos.logback.classic.LoggerContext;

public class ScoringHarnessTool {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private String[] args;
    private ArgumentParser parser;
    private Namespace namespace;
    private AnnotationConfigApplicationContext context;

    private ScoringHarnessTool(String[] args) {
        this.args = args;
        this.parser = ArgumentParsers.newArgumentParser("ScoringHarnessTool");
        this.parser
                .addArgument("-generate")
                .action(Arguments.storeTrue())
                .help("Generates leads and places them in the queue.  Only use for performance testing - expected scores are not asserted.");
        this.parser.addArgument("-load").action(Arguments.storeTrue())
                .help("Load leads and expected scores into the queue.");
        this.parser.addArgument("-daemon").action(Arguments.storeTrue()).help("Run the scoring harness daemon");

        // Initialize Spring
        context = new AnnotationConfigApplicationContext();
        context.scan("com.latticeengines.scoringharness");
        context.refresh();
    }

    public static void main(String[] args) throws Exception {
        ScoringHarnessTool tool = new ScoringHarnessTool(args);
        tool.execute();
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
                if (loggerFactory instanceof LoggerContext) {
                    ((LoggerContext) loggerFactory).stop();
                }
            }
        });
    }

    private void execute() {
        // TODO decouple logging from console output by printing some of the
        // below directly to console
        // and redirecting logging to a file
        log.info("Executing ScoringHarnessTool");
        try {
            parseArguments();
            executeArguments();
        } catch (HelpScreenException e) {
            System.exit(0);
        } catch (ArgumentParserException e) {
            log.error("Error parsing input arguments", e);
            parser.printHelp();
        } catch (Exception e) {
            log.error("Error running ScoringHarnessTool", e);
        }
        log.info("Completed execution of ScoringHarnessTool");
    }

    private void parseArguments() throws ArgumentParserException {
        namespace = parser.parseArgs(args);
        // TODO Should have really used argparse4j subcommands here
        if (!namespace.getBoolean("load") && !namespace.getBoolean("generate") && !namespace.getBoolean("daemon")
                && !namespace.getBoolean("initializeDB")) {
            throw new ArgumentParserException("Must specify a subcommand", parser);
        }
    }

    private void executeArguments() {
        if (namespace.getBoolean("load")) {
            executeLoad();
        } else if (namespace.getBoolean("generate")) {
            executeGenerate();
        } else if (namespace.getBoolean("daemon")) {
            executeDaemon();
        } else if (namespace.getBoolean("initializeDB")) {
            executeInitializeDB();
        }
    }

    private void executeGenerate() {
    }

    private void executeLoad() {
    }

    private void executeDaemon() {
    }

    private void executeInitializeDB() {
    }
}
