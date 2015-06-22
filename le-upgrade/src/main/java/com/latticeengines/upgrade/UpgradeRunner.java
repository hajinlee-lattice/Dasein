package com.latticeengines.upgrade;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.latticeengines.upgrade.service.UpgradeService;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.choice.CollectionArgumentChoice;
import net.sourceforge.argparse4j.inf.ArgumentChoice;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class UpgradeRunner {

    private static ArgumentParser parser;

    private UpgradeService upgrader;

    static {
        parser = ArgumentParsers.newArgumentParser("upgarde");

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
    }

    private static ArgumentChoice getCommandChoice() {
        return new CollectionArgumentChoice<>(
                "modelinfo",
                "cparts"
        );
    }

    private static String commandHelper() {
        String helper = "command to be executed:";
        helper += "\nmodelinfo: populate ModelInfo table";
        helper += "\ncparts:    copy artifacts from 1-id to 3-id folder";
        return helper;
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

            String version = ns.getString("version");

            if (version.startsWith("1.4")) {
                upgrader.switchToVersion("1.4.0");
            } else {
                upgrader.switchToVersion("1.3.4");
            }
            String command = ns.getString("command");

            System.out.println("\n\n========================================");
            System.out.println("Upgrader");
            System.out.println("========================================\n");

            upgrader.execute(command, ns.getAttrs());

            System.out.println("\n\n========================================\n");

        } catch (ArgumentParserException e) {
            parser.handleError(e);

            System.out.println("\n\n========================================");
            System.out.println("Upgrader help");
            System.out.println("========================================\n");
            parser.printHelp();
            System.out.println("\n\n========================================\n");
        }
    }
}
