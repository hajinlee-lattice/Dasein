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

    public static final String CMD_MODEL_INFO = "modelinfo";
    public static final String CMD_LIST = "list";
    public static final String CMD_CP_MODELS = "cp_models";
    public static final String CMD_UPGRADE = "upgrade";

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
                .help("model guid or uuid.");

        parser.addArgument("-a", "--all")
                .dest("all")
                .action(Arguments.storeConst())
                .setConst(true)
                .setDefault(false)
                .help("show all models in hdsf, or upgarde all customers in ModelInfo table.");
    }

    private static ArgumentChoice getCommandChoice() {
        return new CollectionArgumentChoice<>(
                CMD_MODEL_INFO,
                CMD_LIST,
                CMD_CP_MODELS,
                CMD_UPGRADE
        );
    }

    private static String commandHelper() {
        String helper = "command to be executed:";
        helper += "\n " + CMD_MODEL_INFO + ":    populate ModelInfo table for all tenants";
        helper += "\n " + CMD_LIST + ":    list (tenant, model) pairs to be upgraded. " +
                "With the flag -a or --all it shows all models in hdfs";
        helper += "\n " + CMD_CP_MODELS + ":    copy ALL models of ONE customer to 3-id folder in hdfs. " +
                "Also fix the filename of model.json.";
        helper += "\n " + CMD_UPGRADE + ":    end to end upgrade a tenant";
        return helper;
    }

    private List<String> cmdsNeedCustomer() {
        return Arrays.asList(CMD_CP_MODELS, "upgrade");
    }

    private List<String> cmdsNeedModel() {
        return Arrays.asList(CMD_CP_MODELS);
    }

    private void validateArguments(Namespace ns) {
        if (ns == null) {
            throw new IllegalArgumentException("Failed to parse input arguments.");
        }

        String command = ns.getString("command");

        if (cmdsNeedCustomer().contains(command) &&
                !ns.getBoolean("all") && ns.getString("customer") == null ) {
            throw new IllegalArgumentException("Missing customer (tenantId).");
        }

        if (cmdsNeedModel().contains(command) &&
                !ns.getBoolean("all") && ns.getString("model") == null ) {
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
