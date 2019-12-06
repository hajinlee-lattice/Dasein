package com.latticeengines.swlib.service.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.swlib.SoftwarePackage;
import com.latticeengines.domain.exposed.swlib.SoftwarePackageInitializer;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;

@Component("softwareLibraryService")
public class SoftwareLibraryServiceImpl implements SoftwareLibraryService, InitializingBean {
    private static final Logger log = LoggerFactory.getLogger(SoftwareLibraryServiceImpl.class);

    private static final String SWLIB_DISABLED = "LE_SWLIB_DISABLED";

    @Inject
    private Configuration yarnConfiguration;

    private String topLevelPath;

    @Inject
    public SoftwareLibraryServiceImpl(VersionManager versionManager) {
        String stack = PropertyUtils.getProperty("dataplatform.hdfs.stack");
        if (stack == null) {
            stack = "default";
        }
        topLevelPath = "/app/" + stack + "/" + versionManager.getCurrentVersion() + "/swlib";
        log.info("Set top level path to " + topLevelPath);
    }

    @Override
    public String getTopLevelPath() {
        return topLevelPath;
    }

    @Override
    public void setStackAndVersion(String stackName, String version) {
        topLevelPath = "/app/" + stackName + "/" + version + "/swlib";
        log.info("Set top level path to " + topLevelPath);
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
    public void afterPropertiesSet() {
        boolean disabled = Boolean.parseBoolean(System.getenv(SWLIB_DISABLED));
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
            log.warn(e.getMessage(), e);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return packages;
    }

    private SoftwarePackage getInstalledPackageByNameVersion(String module, String name) {
        List<SoftwarePackage> packages = getInstalledPackages(module);
        for (SoftwarePackage pkg : packages) {
            if (name.equals(pkg.getName())) {
                return pkg;
            }
        }
        return null;
    }

    @Override
    public ApplicationContext loadSoftwarePackages(String module, ApplicationContext context) {
        log.info("Did not specify a pkg name, loading all libraries.");
        return loadSoftwarePackages(module, Arrays.asList(SoftwareLibrary.values()), context);
    }

    @Override
    public ApplicationContext loadSoftwarePackages(String module, Collection<String> names, ApplicationContext context) {
        return loadSoftwarePackages(module, names.stream().map(SoftwareLibrary::fromName).collect(Collectors.toList()),
                context);
    }

    private ApplicationContext loadSoftwarePackages(String module, List<SoftwareLibrary> swlibs,
            ApplicationContext context) {
        List<SoftwareLibrary> deps = SoftwareLibrary.getLoadingSequence(SoftwareLibrary.Module.valueOf(module), swlibs);
        return loadSoftwarePackagesInSequence(module, deps, context);
    }

    private ApplicationContext loadSoftwarePackagesInSequence(String module, List<SoftwareLibrary> deps,
            ApplicationContext context) {
        log.info("There are " + deps.size() + " libraries to load, the loading sequence is "
                + StringUtils.join(deps.stream().map(SoftwareLibrary::getName).collect(Collectors.toList()), " -> "));
        for (SoftwareLibrary dep : deps) {
            SoftwarePackage pkg = getInstalledPackageByNameVersion(module, dep.getName());
            if (pkg == null) {
                log.warn("Cannot find software package named " + dep.getName() + " for module " + module + ". Skip");
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
