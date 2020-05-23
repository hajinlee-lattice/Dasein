package com.latticeengines.swlib;

import java.io.File;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.swlib.SoftwarePackage;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;

public final class SwlibTool {

    protected SwlibTool() {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("resource")
    public static void main(String[] args) {
        ClassPathXmlApplicationContext appContext = new ClassPathXmlApplicationContext("swlib-commandline-context.xml");

        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption("o", "operation", true, " -<operation> (install or uninstall or config)"). //
                addOption("n", "name", true, " -<name> (package name - cdl, leadprioritization or datacloud)"). //
                addOption("g", "groupId", true, " -<groupId> (group id Maven-style)"). //
                addOption("a", "artifactId", true, " -<artifactId> (artifact id Maven-style)"). //
                addOption("s", "stack", true, " -<stack> (stackName: a or b)"). //
                addOption("v", "version", true, " -<version> (version Maven-style)"). //
                addOption("c", "classifier", false, " -<classifier> (classifier Maven-style)"). //
                addOption("f", "localFileName", true, " -<localFileName> (path to local file to install)"). //
                addOption("i", "initializer", true, " -<initializer> (initializer class name)"). //
                addOption("h", "defaultFS", true, "-<defaultFS> (Hadoop fs.defaultFS)"); //

        try {
            CommandLine cmd = parser.parse(options, args);
            if (cmd.hasOption("operation")) {
                String operation = cmd.getOptionValue("operation");
                String name = cmd.getOptionValue("name");
                String groupId = cmd.getOptionValue("groupId");
                String artifactId = cmd.getOptionValue("artifactId");
                String version = cmd.getOptionValue("version");
                String stackName = cmd.getOptionValue("stack");
                String classifier = cmd.getOptionValue("classifier");
                String initializerClassName = cmd.getOptionValue("initializer");
                String fsDefaultFS = cmd.getOptionValue("defaultFS");

                File fileToInstall = null;
                SoftwareLibraryService swlibService = appContext.getBean("softwareLibraryService",
                        SoftwareLibraryService.class);
                swlibService.setStackAndVersion(stackName, version);

                SoftwarePackage swPackage = new SoftwarePackage();
                swPackage.setName(name);
                swPackage.setGroupId(groupId);
                swPackage.setArtifactId(artifactId);
                swPackage.setClassifier(classifier);
                swPackage.setInitializerClass(initializerClassName);

                switch (operation) {
                    case "config":
                        ObjectMapper om = new ObjectMapper();
                        SoftwareLibrary swLib = SoftwareLibrary.fromName(swPackage.getName());
                        if (swLib == null) {
                            throw new RuntimeException("Cannot find software library for name " + swPackage.getName());
                        }
                        Set<SoftwareLibrary.Module> modules = swLib.getModules();
                        for (SoftwareLibrary.Module module : modules) {
                            swPackage.setModule(module.name());
                            String outputJson = String.format("target/swlib/%s/%s/%s.json", module, artifactId, artifactId);
                            FileUtils.forceMkdirParent(new File(outputJson));
                            om.writeValue(new File(outputJson), swPackage);
                        }
                        break;
                    case "install":
                        String fileName = cmd.getOptionValue("localFileName");
                        fileToInstall = new File(fileName);
                        if (!fileToInstall.exists()) {
                            throw new Exception(String.format("File %s does not exist.", fileName));
                        }
                        installForAllModules(swlibService, fsDefaultFS, swPackage, fileToInstall);
                        break;
                    case "uninstall":
                        break;
                    default:
                }
            }
        } catch (Exception e) {
            System.out.println(ExceptionUtils.getStackTrace(e));
        }
    }

    private static void installForAllModules(SoftwareLibraryService swlibService, String fsDefaultFS,
            SoftwarePackage swPackage, File fileToInstall) {
        SoftwareLibrary swLib = SoftwareLibrary.fromName(swPackage.getName());
        if (swLib == null) {
            throw new RuntimeException("Cannot find software library for name " + swPackage.getName());
        }
        Set<SoftwareLibrary.Module> modules = swLib.getModules();
        for (SoftwareLibrary.Module module : modules) {
            swPackage.setModule(module.name());
            if (fsDefaultFS != null) {
                swlibService.installPackage(fsDefaultFS, swPackage, fileToInstall);
            } else {
                swlibService.installPackage(swPackage, fileToInstall);
            }
        }
    }

}
