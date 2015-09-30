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
    public static final String CMD_REGISTER_ZK = "register_zk";
    public static final String CMD_REGISTER_PLS = "register_pls";
    public static final String CMD_UPDATE_ACTIVE = "update_activity";
    public static final String CMD_UPGRADE = "upgrade";
    public static final String CMD_SUMMARY = "summary";

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

        parser.addArgument("-c", "--customer")
                .dest("customer")
                .type(String.class)
                .help("customer (tenantId).");

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
                CMD_REGISTER_ZK,
                CMD_REGISTER_PLS,
                CMD_UPDATE_ACTIVE,
                CMD_UPGRADE,
                CMD_SUMMARY
        );
    }

    private static String commandHelper() {
        String helper = "command to be executed:";
        helper += "\n " + CMD_MODEL_INFO + ":    populate ModelInfo table for all tenants";
        helper += "\n " + CMD_LIST + ":    list (tenant, model) pairs to be upgraded. " +
                "With the flag -a or --all it shows all models in hdfs";
        helper += "\n " + CMD_CP_MODELS + ":    copy ALL models of ONE customer to 3-id folder in hdfs. " +
                "Also fix the filename of model.json.";
        helper += "\n " + CMD_REGISTER_ZK + ":    register a tenant in ZK.";
        helper += "\n " + CMD_REGISTER_PLS + ":    register a tenant in PLS/GA.";
        helper += "\n " + CMD_UPDATE_ACTIVE + ":    update activity status of models.";
        helper += "\n " + CMD_UPGRADE + ":    end to end upgrade a tenant";
        helper += "\n " + CMD_SUMMARY + ":    upload upgrade summary";
        return helper;
    }

    private List<String> cmdsNeedCustomer() {
        return Arrays.asList(CMD_CP_MODELS, CMD_REGISTER_ZK, CMD_REGISTER_PLS, CMD_UPGRADE, CMD_UPDATE_ACTIVE, CMD_SUMMARY);
    }

    private List<String> cmdsAllowAll() {
        return Arrays.asList(CMD_LIST, CMD_UPGRADE);
    }

    private void validateArguments(Namespace ns) {
        if (ns == null) {
            throw new IllegalArgumentException("Failed to parse input arguments.");
        }

        String command = ns.getString("command");

        if (cmdsNeedCustomer().contains(command) && ns.getString("customer") == null &&
                !(cmdsAllowAll().contains(command) && ns.getBoolean("all"))) {
            throw new IllegalArgumentException("Missing customer (tenantId).");
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
        try {

            Namespace ns = parser.parseArgs(args);

            validateArguments(ns);

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
