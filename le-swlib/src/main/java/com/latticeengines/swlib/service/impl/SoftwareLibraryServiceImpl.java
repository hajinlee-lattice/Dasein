package com.latticeengines.swlib.service.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.swlib.SoftwarePackage;
import com.latticeengines.domain.exposed.swlib.SoftwarePackageInitializer;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;

@Component("softwareLibraryService")
public class SoftwareLibraryServiceImpl implements SoftwareLibraryService, InitializingBean {
    private static final Log log = LogFactory.getLog(SoftwareLibraryServiceImpl.class);

    private static final String SWLIB_DISABLED = "LE_SWLIB_DISABLED";

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${dataplatform.hdfs.stack:}")
    private String stackName;

    private String topLevelPath = "/app/swlib";

    public SoftwareLibraryServiceImpl() {
        setStackName(stackName);
    }

    @Override
    public String getTopLevelPath() {
        return topLevelPath;
    }

    @Override
    public void setStackName(String stackName) {
        if (StringUtils.isNotEmpty(stackName)) {
            topLevelPath = "/app/" + stackName + "/swlib";
            log.info("Set top level path to " + topLevelPath);
        }
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
        String hdfsJarPath = String.format("%s/%s", topLevelPath, swPackage.getHdfsPath());
        String hdfsJsonPath = String.format("%s/%s", topLevelPath, swPackage.getHdfsPath("json"));
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
        Boolean disabled = Boolean.parseBoolean(System.getenv(SWLIB_DISABLED));
        if (!disabled) {
            createSoftwareLibDir(topLevelPath);
            yarnConfiguration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        }
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
    public List<SoftwarePackage> getLatestInstalledPackages(String module) {
        List<SoftwarePackage> packages = getInstalledPackages(module);

        Map<String, List<SoftwarePackage>> packagesByGroupAndArtifact = new HashMap<>();

        for (SoftwarePackage pkg : packages) {
            String key = String.format("%s.%s", pkg.getGroupId(), pkg.getArtifactId());
            List<SoftwarePackage> list = packagesByGroupAndArtifact.computeIfAbsent(key, k -> new ArrayList<>());
            list.add(pkg);
        }

        Collection<List<SoftwarePackage>> values = packagesByGroupAndArtifact.values();
        packages = new ArrayList<>();
        for (List<SoftwarePackage> value : values) {
            value.sort((o1, o2) -> o2.getVersion().compareTo(o1.getVersion()));
            packages.add(value.get(0));
        }

        return packages;
    }

    @Override
    public List<SoftwarePackage> getInstalledPackagesByVersion(String module, String version) {
        List<SoftwarePackage> packages = getInstalledPackages(module);
        List<SoftwarePackage> versionMatchedPackages = new ArrayList<>();
        for (SoftwarePackage pkg : packages) {
            if (StringUtils.isNotBlank(version) && pkg.getVersion().equals(version)) {
                versionMatchedPackages.add(pkg);
            }
        }
        return versionMatchedPackages;
    }

    @Override
    public List<SoftwarePackage> getInstalledPackages(String module) {
        List<SoftwarePackage> packages = new ArrayList<>();
        try {
            List<String> files = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, //
                    String.format("%s/%s", topLevelPath, module), //
                    file -> file.getPath().toString().endsWith(".json"));

            for (String file : files) {
                try {
                    packages.add(JsonUtils.deserialize(HdfsUtils.getHdfsFileContents(yarnConfiguration, file),
                            SoftwarePackage.class));
                } catch (Exception e) {
                    log.warn(LedpException.buildMessageWithCode(LedpCode.LEDP_27003, new String[] { file }));
                }
            }
        } catch (FileNotFoundException e) {
            log.warn(e.getMessage());
        } catch (Exception e) {
            log.error(e);
        }
        return packages;
    }

    private SoftwarePackage getInstalledPackageByNameVersion(String module, String name, String version) {
        List<SoftwarePackage> packages = getInstalledPackages(module);
        for (SoftwarePackage pkg : packages) {
            if (!pkg.getName().equals(name)) {
                continue;
            }
            if (StringUtils.isBlank(version) || version.equals(pkg.getVersion())) {
                return pkg;
            }
        }
        return null;
    }

    @Override
    public ApplicationContext loadSoftwarePackages(String module, ApplicationContext context,
            VersionManager versionManager) {
        log.info("Did not specify a pkg name, loading all libraries.");
        List<SoftwareLibrary> deps = SoftwareLibrary.getLoadingSequence(SoftwareLibrary.Module.valueOf(module),
                Arrays.asList(SoftwareLibrary.values()));
        return loadSoftwarePackagesInSequence(module, deps, context, versionManager.getCurrentVersion());
    }

    public ApplicationContext loadSoftwarePackages(String module, String name, ApplicationContext context,
            VersionManager versionManager) {
        SoftwareLibrary lib = SoftwareLibrary.fromName(name);
        log.info("Trying to load software libraries for " + lib.getName());
        List<SoftwareLibrary> deps = lib.getLoadingSequence(SoftwareLibrary.Module.valueOf(module));
        return loadSoftwarePackagesInSequence(module, deps, context, versionManager.getCurrentVersion());
    }

    private ApplicationContext loadSoftwarePackagesInSequence(String module, List<SoftwareLibrary> deps,
            ApplicationContext context, String version) {
        log.info("There are " + deps.size() + " libraries to load, the loading sequence is "
                + StringUtils.join(deps.stream().map(SoftwareLibrary::getName).collect(Collectors.toList()), " -> "));
        for (SoftwareLibrary dep : deps) {
            SoftwarePackage pkg = getInstalledPackageByNameVersion(module, dep.getName(), version);
            if (pkg == null) {
                log.warn("Cannot find software package named " + dep.getName() + " for module " + module
                        + " at version [" + version + "]. Skip");
            } else {
                log.info(String.format("Classpath = %s", System.getenv("CLASSPATH")));
                log.info(String.format("Found software package %s from the software library for this module %s.",
                        pkg.getName(), pkg.getModule()));
                String initializerClassName = pkg.getInitializerClass();
                log.info(String.format("Loading %s", initializerClassName));
                SoftwarePackageInitializer initializer;
                try {
                    Class<?> c = Class.forName(initializerClassName);
                    initializer = (SoftwarePackageInitializer) c.newInstance();
                    context = initializer.initialize(context, module);
                } catch (Exception e) {
                    log.error(LedpException.buildMessage(LedpCode.LEDP_27004, new String[] { initializerClassName }),
                            e);
                }
            }
        }
        return context;
    }

}
