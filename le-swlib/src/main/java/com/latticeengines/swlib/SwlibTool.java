package com.latticeengines.swlib;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.latticeengines.domain.exposed.swlib.SoftwarePackage;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;

public class SwlibTool {

    @SuppressWarnings("resource")
    public static void main(String[] args) {
        ClassPathXmlApplicationContext appContext = new ClassPathXmlApplicationContext("swlib-commandline-context.xml");

        CommandLineParser parser = new PosixParser();
        Options options = new Options();
        options.addOption("o", "operation", true, " -<operation> (install or uninstall)"). //
                addOption("m", "module", true, " -<module> (module name - dataflow or workflow)"). //
                addOption("g", "groupId", true, " -<groupId> (group id Maven-style)"). //
                addOption("a", "artifactId", true, " -<artifactId> (artifact id Maven-style)"). //
                addOption("s", "stack", true, " -<stack> (stackName: a or b)"). //
                addOption("v", "version", true, " -<version> (version Maven-style)"). //
                addOption("c", "classifier", false, " -<classifier> (classifier Maven-style)"). //
                addOption("f", "localFileName", true, " -<localFileName> (path to local file to install)"). //
                addOption("i", "initializer", true, " -<initializer> (initializer class name)"). //
                addOption("h", "defaultFS", true, "-<defaultFS> (Hadoop fs.defaultFS)");

        try {
            CommandLine cmd = parser.parse(options, args);
            if (cmd.hasOption("operation")) {
                String operation = cmd.getOptionValue("operation");
                String module = cmd.getOptionValue("module");
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
                swlibService.setStackName(stackName);

                SoftwarePackage swPackage = new SoftwarePackage();
                swPackage.setModule(module);
                swPackage.setGroupId(groupId);
                swPackage.setArtifactId(artifactId);
                swPackage.setVersion(version);
                swPackage.setClassifier(classifier);
                swPackage.setInitializerClass(initializerClassName);

                if (operation.equals("install")) {
                    String fileName = cmd.getOptionValue("localFileName");
                    fileToInstall = new File(fileName);

                    if (!fileToInstall.exists()) {
                        throw new Exception(String.format("File %s does not exist.", fileName));
                    }
                    if (fsDefaultFS != null) {
                        swlibService.installPackage(fsDefaultFS, swPackage, fileToInstall);
                    } else {
                        swlibService.installPackage(swPackage, fileToInstall);
                    }
                    

                } else if (operation.equals("uninstall")) {
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
