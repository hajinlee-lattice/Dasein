package com.latticeengines.upgrade.model.runner;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.latticeengines.upgrade.model.service.ModelUpgradeService;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.choice.CollectionArgumentChoice;
import net.sourceforge.argparse4j.inf.ArgumentChoice;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class ModelUpgradeRunner {

    private static ArgumentParser parser;

    private ModelUpgradeService upgrader134;
    private ModelUpgradeService upgrader140;

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
        String helper = "command to be executed:\n";
        helper += "modelinfo: populate ModelInfo table\n";
        helper += "cparts: copy artifacts from 1-id to 3-id folder\n";
        return helper;
    }

    public ModelUpgradeRunner(){
        @SuppressWarnings("resource")
        ApplicationContext ac = new ClassPathXmlApplicationContext("upgrade-context.xml");
        this.upgrader134 = (ModelUpgradeService) ac.getBean("model_134_Upgrade");
        this.upgrader140 = (ModelUpgradeService) ac.getBean("model_140_Upgrade");
    }

    public static void main(String[] args) throws Exception {
        ModelUpgradeRunner runner = new ModelUpgradeRunner();
        runner.run(args);
    }

    private void run(String[] args) throws Exception {
        //modelUgprade.upgrade();
        try {

            Namespace ns = parser.parseArgs(args);

            String version = ns.getString("version");

            ModelUpgradeService modelUpgrade;
            if (version.startsWith("1.4")) {
                modelUpgrade = this.upgrader140;
            } else {
                modelUpgrade = this.upgrader134;
            }
            String command = ns.getString("command");

            modelUpgrade.execute(command, ns.getAttrs());

        } catch (ArgumentParserException e) {
            parser.handleError(e);
        }
    }
}
