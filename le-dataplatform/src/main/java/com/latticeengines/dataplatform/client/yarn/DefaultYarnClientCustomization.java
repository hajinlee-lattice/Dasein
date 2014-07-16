package com.latticeengines.dataplatform.client.yarn;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.springframework.yarn.fs.DefaultResourceLocalizer;
import org.springframework.yarn.fs.LocalResourcesFactoryBean;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.TransferEntry;
import org.springframework.yarn.fs.ResourceLocalizer;

import com.latticeengines.dataplatform.runtime.python.PythonContainerProperty;

public class DefaultYarnClientCustomization implements YarnClientCustomization {
    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(DefaultYarnClientCustomization.class);

    protected Configuration configuration;
    
    protected String hdfsJobBaseDir;

    public DefaultYarnClientCustomization(Configuration configuration, String hdfsJobBaseDir) {
        this.configuration = configuration;
        this.hdfsJobBaseDir = hdfsJobBaseDir;
    }

    @Override
    public ResourceLocalizer getResourceLocalizer(Properties containerProperties) {
        return new DefaultResourceLocalizer(configuration, getHdfsEntries(containerProperties),
                getCopyEntries(containerProperties));
    }

    protected String getJobDir(Properties containerProperties) {
        return hdfsJobBaseDir+"/"+containerProperties.getProperty(ContainerProperty.JOBDIR.name());
    }

    @Override
    public Collection<CopyEntry> getCopyEntries(Properties containerProperties) {
        Collection<LocalResourcesFactoryBean.CopyEntry> copyEntries = new ArrayList<LocalResourcesFactoryBean.CopyEntry>();
        String containerLaunchContextFile = containerProperties.getProperty(ContainerProperty.APPMASTER_CONTEXT_FILE
                .name());
        copyEntries.add(new LocalResourcesFactoryBean.CopyEntry("file:" + containerLaunchContextFile,
                getJobDir(containerProperties), false));

        return copyEntries;
    }

    @Override
    public Collection<TransferEntry> getHdfsEntries(Properties containerProperties) {
        Collection<LocalResourcesFactoryBean.TransferEntry> hdfsEntries = new ArrayList<LocalResourcesFactoryBean.TransferEntry>();
        hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(LocalResourceType.FILE, //
                LocalResourceVisibility.PUBLIC, //
                "/lib/*", //
                false));
        hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(LocalResourceType.FILE, //
                LocalResourceVisibility.PUBLIC, //
                "/app/dataplatform/*.properties", //
                false));
        hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(LocalResourceType.FILE, //
                LocalResourceVisibility.PUBLIC, //
                "/app/dataplatform/*.jar", //
                false));
        hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(LocalResourceType.FILE, //
                LocalResourceVisibility.PUBLIC, //
                getJobDir(containerProperties) + "/*", //
                false));
        return hdfsEntries;
    }

    @Override
    public String getClientId() {
        return "defaultYarnClient";
    }

    @Override
    public int getMemory(Properties properties) {
        String memory = properties.getProperty(AppMasterProperty.MEMORY.name(), "-1");
        return Integer.parseInt(memory);
    }

    @Override
    public int getPriority(Properties properties) {
        String priority = properties.getProperty(AppMasterProperty.PRIORITY.name(), "-1");
        return Integer.parseInt(priority);
    }

    @Override
    public String getQueue(Properties properties) {
        return properties.getProperty(AppMasterProperty.QUEUE.name());
    }

    @Override
    public int getVirtualcores(Properties properties) {
        String virtualCores = properties.getProperty(AppMasterProperty.VIRTUALCORES.name(), "-1");
        return Integer.parseInt(virtualCores);
    }

    @Override
    public String getContainerLauncherContextFile(Properties properties) {
        return "/default/dataplatform-default-appmaster-context.xml";
    }

    @Override
    public List<String> getCommands(Properties containerProperties) {
        String containerLaunchContextFile = containerProperties.getProperty(ContainerProperty.APPMASTER_CONTEXT_FILE
                .name());
        if (containerLaunchContextFile == null) {
            throw new IllegalStateException("Property " + ContainerProperty.APPMASTER_CONTEXT_FILE + " does not exist.");
        }
        File contextFile = new File(containerLaunchContextFile);
        if (!contextFile.exists()) {
            throw new IllegalStateException("Container launcher context file " + containerLaunchContextFile
                    + " does not exist.");
        }
        String parameter = setupParameters(containerProperties);

        return Arrays.<String> asList(new String[] { "$JAVA_HOME/bin/java", //
                // "-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=4001,server=y,suspend=y",
                // //
                "org.springframework.yarn.am.CommandLineAppmasterRunnerForLocalContextFile", //
                contextFile.getName(), //
                "yarnAppmaster", //
                 parameter, //
                "1><LOG_DIR>/Appmaster.stdout", //
                "2><LOG_DIR>/Appmaster.stderr" });
    }

    @Override
    public void beforeCreateLocalLauncherContextFile(Properties properties) {
    }

    @Override
    public void validate(Properties appMasterProperties, Properties containerProperties) {
    }

    @Override
    public void finalize(Properties appMasterProperties, Properties containerProperties) {
    }
    
    private String setupParameters(Properties containerProperties) {
        Properties prop = new Properties();
        for (Map.Entry<Object, Object> entry : containerProperties.entrySet()) {
            // Exclude METADATA_CONTENT to avoid nested properties
            if (!entry.getKey().toString().equals(PythonContainerProperty.METADATA_CONTENTS.name())) {
                prop.put(entry.getKey(), entry.getValue());
            }
        }
        String propStr = prop.toString();
        return propStr.substring(1, propStr.length() - 1).replaceAll(",", " ");
    }

}
