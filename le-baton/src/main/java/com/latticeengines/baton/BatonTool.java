package com.latticeengines.baton;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.CamilleConfiguration;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.CamilleEnvironment.Mode;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;

public class BatonTool {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private static BatonService batonService = new BatonServiceImpl();

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

    // XXX This needs to be cleaned up to use subcommands and also to have a
    // single try/catch block
    public static void main(String[] args) {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("Baton");

        parser.addArgument("--podId").help("Camille PodId");
        parser.addArgument("--connectionString", "--cs").help("Connection string for ZooKeeper");
        parser.addArgument("--camilleJson").help(
                "Path to camille.json.  Specify instead of explicitly specifying podId and connectionString.");

        Subparsers subparsers = parser.addSubparsers().dest("command");
        Subparser createPod = subparsers.addParser("createPod").help("Creates a new Pod.");
        Subparser createTenant = subparsers.addParser("createTenant").help(
                "Creates a new tenant. Requires contractId, tenantID, spaceId, featureFlags, and properties");

        createTenant.addArgument("--contractId").required(true);
        createTenant.addArgument("--tenantId").required(true);
        createTenant.addArgument("--spaceId").required(true);
        createTenant
                .addArgument("--featureFlags")
                .required(true)
                .help("File containing the feature flags to use for the default customer space created for this tenant");
        createTenant.addArgument("--properties").required(true)
                .help("File containing the properties to use for the default customer space created for this tenant");

        // Don't let PLO know about this...
        Subparser loadDirectory = subparsers.addParser("loadDirectory").help("Only for development use!");
        loadDirectory.addArgument("--source", "--S", "--s").required(true).help(Arguments.SUPPRESS);
        loadDirectory.addArgument("--destination", "--D", "--d").required(true).help(Arguments.SUPPRESS);

        Namespace namespace = null;
        try {
            namespace = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            log.error("Error parsing input arguments", e.getMessage());
            System.exit(1);
        }

        String connectionString = (String) namespace.get("connectionString");
        String podId = (String) namespace.get("podId");
        String camilleJsonPath = (String) namespace.get("camilleJson");

        try {
            CamilleConfiguration config;
            if (camilleJsonPath != null) {
                config = new ObjectMapper().readValue(new File(camilleJsonPath), CamilleConfiguration.class);
            } else {
                config = new CamilleConfiguration();
                config.setConnectionString(connectionString);
                config.setPodId(podId);
            }

            CamilleEnvironment.start(Mode.BOOTSTRAP, config);

        } catch (Exception e) {
            log.error("Error starting Camille", e);
            System.exit(1);
        }

        if (namespace.get("command").equals("loadDirectory")) {
            String source = namespace.get("source");
            String destination = namespace.get("destination");

            if (source == null || destination == null) {
                log.error("LoadDirectory requires source and destination");
                System.exit(1);
            }

            else {
                batonService.loadDirectory(source, destination);
            }
        }

        else if (namespace.get("command").equals("createPod")) {
            log.info(String.format("Sucessfully created pod %s", podId));
        }

        else if (namespace.get("command").equals("createTenant")) {
            String contractId = namespace.get("contractId");
            String tenantId = namespace.get("tenantId");
            String spaceId = namespace.get("spaceId");

            String flagsFilename = namespace.get("featureFlags");
            String propertiesFilename = namespace.get("properties");

            String flags = null;
            try {
                flags = new String(Files.readAllBytes(Paths.get(flagsFilename)), StandardCharsets.UTF_8);
            } catch (IOException e) {
                log.error(String.format("Exception encountered reading file %s: %s", flagsFilename, e.getMessage()), e);
                System.exit(1);
            }

            CustomerSpaceProperties properties = null;
            try {
                String propertiesJson = new String(Files.readAllBytes(Paths.get(propertiesFilename)),
                        StandardCharsets.UTF_8);
                properties = new ObjectMapper().readValue(propertiesJson, CustomerSpaceProperties.class);
            } catch (IOException e) {
                log.error(
                        String.format("Exception encountered reading file %s: %s", propertiesFilename, e.getMessage()),
                        e);
                System.exit(1);
            }

            batonService.createTenant(contractId, tenantId, spaceId, new CustomerSpaceInfo(properties, flags));

        }
    }

    /**
     * Creates the contract if it does not exist, and then creates the tenant
     * and space
     * 
     * @param contractId
     * @param tenantId
     * @param spaceId
     */
    static void createTenant(String contractId, String tenantId, String spaceId, CustomerSpaceInfo spaceInfo) {
        batonService.createTenant(contractId, tenantId, spaceId, spaceInfo);
    }

    /**
     * Loads directory into ZooKeeper
     * 
     * @param source
     *            Path of files to load
     * @param destination
     *            Path in ZooKeeper to store files
     */
    static void loadDirectory(String source, String destination) {
        batonService.loadDirectory(source, destination);
    }

    private static boolean anyNull(String... args) {
        for (String arg : args) {
            if (arg == null) {
                return true;
            }
        }
        return false;
    }
}
