package com.latticeengines.eai.yarn.client;

import java.io.File;
import java.util.Collection;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.yarn.fs.LocalResourcesFactoryBean;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.TransferEntry;

import com.latticeengines.dataplatform.exposed.yarn.client.ContainerProperty;
import com.latticeengines.dataplatform.exposed.yarn.client.DefaultYarnClientCustomization;
import com.latticeengines.eai.yarn.runtime.EaiContainerProperty;

@Component("eaiClientCustomization")
public class EaiClientCustomization extends DefaultYarnClientCustomization {

    private static final Log log = LogFactory.getLog(EaiClientCustomization.class);

    @Autowired
    public EaiClientCustomization(Configuration yarnConfiguration,
            @Value("${dataplatform.yarn.job.basedir}") String hdfsJobBaseDir,
            @Value("${dataplatform.fs.web.defaultFS}") String webHdfs) {
        super(yarnConfiguration, hdfsJobBaseDir, webHdfs);
    }

    @Override
    public String getClientId() {
        return "eaiClient";
    }

    @Override
    public String getContainerLauncherContextFile(Properties properties) {
        return "/eai-appmaster-context.xml";
    }

    @Override
    public void beforeCreateLocalLauncherContextFile(Properties properties) {
        try {
            String dir = properties.getProperty(ContainerProperty.JOBDIR.name());
            String importConfig = (String) properties.remove(EaiContainerProperty.IMPORTCONFIG.name());
            File metadataFile = new File(dir + "/metadata.json");
            FileUtils.writeStringToFile(metadataFile, importConfig);
            properties.put(ContainerProperty.METADATA.name(), metadataFile.getAbsolutePath());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Collection<CopyEntry> getCopyEntries(Properties containerProperties) {
        Collection<CopyEntry> copyEntries = super.getCopyEntries(containerProperties);
        String metadataFilePath = containerProperties.getProperty(ContainerProperty.METADATA.name());
        copyEntries.add(new LocalResourcesFactoryBean.CopyEntry("file:" + metadataFilePath,
                getJobDir(containerProperties), false));
        return copyEntries;
    }

    @Override
    public Collection<TransferEntry> getHdfsEntries(Properties containerProperties) {
        Collection<LocalResourcesFactoryBean.TransferEntry> hdfsEntries = super.getHdfsEntries(containerProperties);
        hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(LocalResourceType.FILE, //
                LocalResourceVisibility.PUBLIC, //
                "/app/dataplatform/eai/eai.properties", //
                false));
        return hdfsEntries;
    }

//    @Override
//    public void validate(Properties appMasterProperties, Properties containerProperties) {
//        String metadata = containerProperties.getProperty(ContainerProperty.METADATA.name());
//        JSONParser parser = new JSONParser();
//        JSONArray objArr = null;
//        try {
//            objArr = (JSONArray) parser.parse(metadata);
//        } catch (ParseException e) {
//            log.error("Cannot create JSONArray Object from the metadata file");
//            log.error(e.getMessage());
//        }
//        List<Table> tables = new ArrayList<>();
//        for (Object obj : objArr.toArray()) {
//            tables.add(JsonUtils.deserialize(obj.toString(), Table.class));
//        }
//
//        for (Table table : tables) {
//            String name = table.getName();
//            if (StringUtils.isEmpty(name)) {
//                throw new LedpException(LedpCode.LEDP_10006);
//            }
//            List<Attribute> attributes = table.getAttributes();
//            if (attributes == null || attributes.size() == 0) {
//                throw new LedpException(LedpCode.LEDP_17000);
//            }
//            for (Attribute attribute : attributes) {
//                if (StringUtils.isEmpty(attribute.getName())) {
//                    throw new LedpException(LedpCode.LEDP_10006);
//                }
//            }
//        }
//
//        String targetPath = containerProperties.getProperty(EaiContainerProperty.TARGET_PATH.name());
//        if (StringUtils.isEmpty(targetPath)) {
//            throw new LedpException(LedpCode.LEDP_10003);
//        }
//    }
}
