package com.latticeengines.dataplatform.functionalframework;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StringUtils;
import org.springframework.yarn.fs.PrototypeLocalResourcesFactoryBean.CopyEntry;

import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.yarn.exposed.client.AppMasterProperty;
import com.latticeengines.yarn.exposed.client.ContainerProperty;

public interface DataplatformFunctionalTestNGInterface {

    default File[] getAvroFilesForDir(String parentDir) {
        return new File(parentDir).listFiles(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".avro");
            }

        });
    }

    default void doCopy(FileSystem fs, List<CopyEntry> copyEntries) throws Exception {
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

        for (CopyEntry e : copyEntries) {
            for (String pattern : StringUtils.commaDelimitedListToStringArray(e.getSrc())) {
                for (Resource res : resolver.getResources(pattern)) {
                    Path destinationPath = getDestinationPath(e, res);
                    FSDataOutputStream os = fs.create(destinationPath);
                    FileCopyUtils.copy(res.getInputStream(), os);
                }
            }
        }

    }

    default Path getDestinationPath(CopyEntry entry, Resource res) throws IOException {
        Path dest = new Path(entry.getDest(), res.getFilename());
        return dest;
    }

    default String getFileUrlFromResource(String resource) {
        URL url = ClassLoader.getSystemResource(resource);
        return "file:" + url.getFile();
    }

    default Properties createAppMasterPropertiesForYarnJob() {
        Properties appMasterProperties = new Properties();
        appMasterProperties.put(AppMasterProperty.QUEUE.name(), LedpQueueAssigner.getModelingQueueNameForSubmission());
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(), "Dell");
        return appMasterProperties;
    }

    default Properties createContainerPropertiesForYarnJob() {
        Properties containerProperties = new Properties();
        containerProperties.put(ContainerProperty.VIRTUALCORES.name(), "1");
        containerProperties.put(ContainerProperty.MEMORY.name(), "64");
        containerProperties.put(ContainerProperty.PRIORITY.name(), "0");
        return containerProperties;
    }

    default String generateUnique(String base) {
        String id = UUID.randomUUID().toString();
        return base.equals("") ? id : (base + "_" + id);
    }

    default String generateUnique() {
        return generateUnique("");
    }

}
