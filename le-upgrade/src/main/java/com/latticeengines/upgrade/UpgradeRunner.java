package com.latticeengines.upgrade;

import java.util.Arrays;
import java.util.List;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.latticeengines.upgrade.service.UpgradeService;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.impl.choice.CollectionArgumentChoice;
import net.sourceforge.argparse4j.inf.ArgumentChoice;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class UpgradeRunner {

    private static ArgumentParser parser;

    private UpgradeService upgrader;

    static {
        parser = ArgumentParsers.newArgumentParser("upgrade");

        parser.description("Upgrade tenants from 1.3.4 or 1.4 to 2.0");

        parser.addArgument("command")
                .metavar("cmd")
                .required(true)
                .choices(getCommandChoice())
                .help(commandHelper());

        parser.addArgument("-v", "--version")
                .dest("version")
                .choices(new CollectionArgumentChoice<>("1.3.4", "1.4.0"))
                .setDefault("1.3.4")
                .help("version before upgrade: 1.3.4 or 1.4.0");

        parser.addArgument("-c", "--customer")
                .dest("customer")
                .type(String.class)
                .help("customer (tenantId).");

        parser.addArgument("-m", "--modelguid")
                .dest("model")
                .type(String.class)
                .help("model guid.");

        parser.addArgument("-a", "--all")
                .dest("all")
                .action(Arguments.storeConst())
                .setConst(true)
                .setDefault(false)
                .help("all customers and their active models.");
    }

    private static ArgumentChoice getCommandChoice() {
        return new CollectionArgumentChoice<>(
                "modelinfo",
                "list",
                "cp_model",
                "cp_data",
                "upgrade"
        );
    }

    private static String commandHelper() {
        String helper = "command to be executed:";
        helper += "\nmodelinfo: populate ModelInfo table for all tenants";
        helper += "\nlist:      list (tenant, model) pairs to be upgraded";
        helper += "\ncp_model:  copy files associated with a model to 3-id folder in hdfs";
        helper += "\ncp_data:   copy a data folder to 3-id folder in hdfs";
        helper += "\nupgrade:   end to end upgrade a tenant";
        return helper;
    }

    private List<String> cmdsNeedCustomer() {
        return Arrays.asList("cp_model", "cp_data", "upgrade");
    }

    private List<String> cmdsNeedModel() {
        return Arrays.asList("cp_model");
    }

    private void validateArguments(Namespace ns) {
        if (ns == null) {
            throw new IllegalArgumentException("Failed to parse input arguments.");
        }

        if (cmdsNeedCustomer().contains(ns.getString("command")) &&
                !ns.getBoolean("all") &&
                ns.getString("customer") == null ) {
            throw new IllegalArgumentException("Missing customer (tenantId).");
        }

        if (cmdsNeedModel().contains(ns.getString("command")) &&
                !ns.getBoolean("all") &&
                ns.getString("model") == null ) {
            throw new IllegalArgumentException("Missing model guid.");
        }
    }

    private void handleException(Exception e) {
        if (e instanceof ArgumentParserException) {
            parser.handleError((ArgumentParserException) e);
        } else {
            parser.printUsage();
            System.out.println("error: " + e.getMessage());
        }

        System.out.println("\n========================================");
        System.out.println("Upgrader help");
        System.out.println("========================================\n");
        parser.printHelp();
        System.out.println("\n\n========================================\n");
    }

    public UpgradeRunner(){
        @SuppressWarnings("resource")
        ApplicationContext ac = new ClassPathXmlApplicationContext("upgrade-context.xml");
        this.upgrader = (UpgradeService) ac.getBean("upgrader");
    }

    public static void main(String[] args) throws Exception {
        UpgradeRunner runner = new UpgradeRunner();
        runner.run(args);
    }

    private void run(String[] args) throws Exception {
        //modelUgprade.upgrade();
        try {

            Namespace ns = parser.parseArgs(args);

            validateArguments(ns);

            String version = ns.getString("version");

            if (version.startsWith("1.4")) {
                upgrader.switchToVersion("1.4.0");
            } else {
                upgrader.switchToVersion(version);
            }
            String command = ns.getString("command");

            System.out.println("\n\n========================================");
            System.out.println("Upgrader");
            System.out.println("========================================\n");

            upgrader.execute(command, ns.getAttrs());

            System.out.println("\n\n========================================\n");

        } catch (ArgumentParserException|IllegalArgumentException e) {
            handleException(e);
        }
    }
}
