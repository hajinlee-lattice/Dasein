package com.latticeengines.camille;

import java.io.StringReader;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.CamilleEnvironment.Mode;

public class App {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("prog");
        parser.addArgument("-connectionString");
        parser.addArgument("-name");

        Namespace namespace = null;
        try {
            namespace = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            log.error("Error parsing arguments", e);
            System.exit(1);
        }

        String name = namespace.get("name");
        if (name != null)
            log.info("Hello, {}!", name);

        String connectionString = namespace.get("connectionString");
        if (connectionString == null)
            connectionString = "127.0.0.1:2181";

        CamilleEnvironment
                .start(Mode.BOOTSTRAP,
                        new StringReader(String.format("{\"podId\":\"ignored\",\"connectionString\":\"%s\"}",
                                connectionString)));

        log.info(CamilleEnvironment.getCamille().getCuratorClient().getState().toString());

        CamilleEnvironment.stop();
    }
}
