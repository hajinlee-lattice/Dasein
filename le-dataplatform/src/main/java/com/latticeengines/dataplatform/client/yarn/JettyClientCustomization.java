package com.latticeengines.dataplatform.client.yarn;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.util.StringUtils;
import org.springframework.yarn.fs.LocalResourcesFactoryBean;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.runtime.jetty.JettyContainerProperty;
import com.latticeengines.dataplatform.runtime.python.PythonContainerProperty;
import com.latticeengines.domain.exposed.modeling.Classifier;
import com.latticeengines.domain.exposed.modeling.DataSchema;
import com.latticeengines.domain.exposed.modeling.Field;

public class JettyClientCustomization extends DefaultYarnClientCustomization {

    public JettyClientCustomization(Configuration configuration, String hdfsJobBaseDir) {
        super(configuration, hdfsJobBaseDir);
    }

    @Override
    public String getClientId() {
        return "jettyClient";
    }

    @Override
    public String getContainerLauncherContextFile(Properties properties) {
        return "/jetty/dataplatform-jetty-appmaster-context.xml";
    }

    @Override
    public void beforeCreateLocalLauncherContextFile(Properties properties) {
        try {
            properties.put(JettyContainerProperty.WAR_FILE.name(), properties.getProperty(ContainerProperty.METADATA.name()) + ".war");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Collection<CopyEntry> getCopyEntries(Properties containerProperties) {
        Collection<CopyEntry> copyEntries = super.getCopyEntries(containerProperties);

        return copyEntries;

    }

    @Override
    public void validate(Properties appMasterProperties, Properties containerProperties) {
    }

}
