package com.latticeengines.scoringharness;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.internal.HelpScreenException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.latticeengines.scoringharness.marketoharness.MarketoOperationFactory;
import com.latticeengines.scoringharness.operationmodel.Operation;
import com.latticeengines.scoringharness.operationmodel.OperationFactory;
import com.latticeengines.scoringharness.operationmodel.OperationSpec;

public class ScoringHarness {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    public static void main(String[] args) {

        ArgumentParser parser = ArgumentParsers.newArgumentParser("ScoringHarness");
        parser.addArgument("input-file").required(true).help("Input command file to process");
        parser.addArgument("output-file").required(true).help("Output file for results");

        Namespace namespace = null;
        try {
            namespace = parser.parseArgs(args);
        } catch (HelpScreenException e) {
            System.exit(0);
        } catch (ArgumentParserException e) {
            log.error("Input arguments are invalid: " + e.getMessage());
            parser.printHelp();
            System.exit(1);
        }

        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.scan("com.latticeengines.scoringharness");
        context.refresh();

        OutputFileWriter writer = null;
        try {
            writer = new OutputFileWriter(namespace.getString("output-file"));
        } catch (IOException e) {
            log.error(String.format("Failed to open file %s for writing", namespace.getString("output-file")), e);
            System.exit(1);
        }

        List<OperationSpec> specs = null;

        try {
            specs = new InputFileReader(namespace.getString("input-file")).read();
        } catch (Exception e) {
            log.error(String.format("Failed to read input file %s", namespace.getString("input-file")), e);
            System.exit(1);
        }

        try {

            OperationFactory factory = new MarketoOperationFactory(context, writer);

            long startTime = new Date().getTime();
            for (OperationSpec spec : specs) {
                long currentTime = new Date().getTime();
                long sleepTime = startTime + spec.offsetMilliseconds - currentTime;
                if (sleepTime < 0) {
                    log.warn(String.format("Running %d milliseconds behind at t=%d!", -sleepTime,
                            spec.offsetMilliseconds));
                } else {
                    Thread.sleep(sleepTime);
                }

                Operation<?> operation = factory.getOperation(spec);
                log.info(String.format("Performing %s at t=%d", operation.getName(), spec.offsetMilliseconds));
                operation.execute();
            }
        } catch (Exception e) {
            log.error("Fatal error encountered running ScoringHarness", e);
            System.exit(1);
        }
    }
}
