package com.latticeengines.swlib.service.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.swlib.SoftwarePackage;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;

@Component("softwareLibraryService")
public class SoftwareLibraryServiceImpl implements SoftwareLibraryService, InitializingBean {
    private static final Log log = LogFactory.getLog(SoftwareLibraryServiceImpl.class);

    @Autowired
    private Configuration yarnConfiguration;

    public SoftwareLibraryServiceImpl() {
    }

    @Override
    public void installPackage(String fsDefaultFS, SoftwarePackage swPackage, File localFile) {
        yarnConfiguration.set("fs.defaultFS", fsDefaultFS);
        installPackage(swPackage, localFile);
    }

    @Override
    public void installPackage(SoftwarePackage swPackage, File localFile) {
        log.info("fs.defaultFS = " + yarnConfiguration.get("fs.defaultFS"));
        String localFilePath = localFile.getAbsolutePath();
        String hdfsJarPath = String.format("%s/%s", TOPLEVELPATH, swPackage.getHdfsPath());
        String hdfsJsonPath = String.format("%s/%s", TOPLEVELPATH, swPackage.getHdfsPath("json"));
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, hdfsJarPath)) {
                throw new LedpException(LedpCode.LEDP_27002, new String[] { hdfsJarPath });
            }
            HdfsUtils.copyLocalToHdfs(yarnConfiguration, localFilePath, hdfsJarPath);
            HdfsUtils.writeToFile(yarnConfiguration, hdfsJsonPath, swPackage.toString());
        } catch (Exception e) {
            if (e instanceof LedpException) {
                throw (LedpException) e;
            }
            throw new LedpException(LedpCode.LEDP_27001, e, new String[] { localFilePath, hdfsJarPath });
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        createSoftwareLibDir(TOPLEVELPATH);
        yarnConfiguration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    }

    protected void createSoftwareLibDir(String dir) {
        try {
            if (!HdfsUtils.fileExists(yarnConfiguration, dir)) {
                HdfsUtils.mkdir(yarnConfiguration, dir);
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_27000, e);
        }

    }

    @Override
    public List<SoftwarePackage> getInstalledPackages(String module) {
        List<SoftwarePackage> packages = new ArrayList<>();
        try {
            List<String> files = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, //
                    String.format("%s/%s", SoftwareLibraryService.TOPLEVELPATH, module), //
                    new HdfsFileFilter() {

                        @Override
                        public boolean accept(FileStatus file) {
                            return file.getPath().toString().endsWith(".json");
                        }
                    });
            
            for (String file : files) {
                try {
                    packages.add(JsonUtils.deserialize(HdfsUtils.getHdfsFileContents(yarnConfiguration, file),
                            SoftwarePackage.class));
                } catch (Exception e) {
                    log.warn(LedpException.buildMessageWithCode(LedpCode.LEDP_27003, new String[] { file }), e);
                }
            }
        } catch (Exception e) {
            log.error(e);
        }
        return packages;
    }
    
    @Override
    public List<SoftwarePackage> getLatestInstalledPackages(String module) {
        List<SoftwarePackage> packages = getInstalledPackages(module);
        
        Map<String, List<SoftwarePackage>> packagesByGroupAndArtifact = new HashMap<>();
        
        for (SoftwarePackage pkg : packages) {
            String key = String.format("%s.%s", pkg.getGroupId(), pkg.getArtifactId());
            List<SoftwarePackage> list = packagesByGroupAndArtifact.get(key);
            
            if (list == null) {
                list = new ArrayList<>();
                packagesByGroupAndArtifact.put(key, list);
            }
            list.add(pkg);
        }
        
        Collection<List<SoftwarePackage>> values = packagesByGroupAndArtifact.values();
        packages = new ArrayList<>();
        for (List<SoftwarePackage> value : values) {
            Collections.sort(value, new Comparator<SoftwarePackage>() {

                @Override
                public int compare(SoftwarePackage o1, SoftwarePackage o2) {
                    return o2.getVersion().compareTo(o1.getVersion());
                }
                
            });
            packages.add(value.get(0));
        }
        
        return packages;
    }

}
