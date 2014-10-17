package com.latticeengines.baton;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    public static void main(String[] args) {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("prog");
        parser.addArgument("-name");

        Namespace namespace = null;
        try {
            namespace = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            System.exit(1);
        }

        String name = namespace.get("name");
        String s = String.format("Hello %s!", name);

        println(s);
        log.info(s);
    }

    private static void println(Object o) {
        System.out.println(o);
    }
}
