package com.latticeengines.admin.dynamicopts.impl;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.dynamicopts.OptionsProvider;
import com.latticeengines.domain.exposed.admin.CRMTopology;

@Component
public class TemplateProvider implements OptionsProvider {

    private static Map<CRMTopology, String> templateMap;

    private MountedDirectoryOptionsProvider dirWatcher;

    private String absoluteRoot;

    @Value("${admin.mount.rootpath}")
    private String mountRoot;

    @Value("${admin.mount.tpl}")
    private String tplPath;

    @PostConstruct
    private void watchDir() {
        if (dirWatcher == null) {
            Path path = FileSystems.getDefault().getPath(mountRoot, tplPath);
            dirWatcher = new MountedDirectoryOptionsProvider(path);
        }
        absoluteRoot = dirWatcher.getAbsoluteRoot();

        templateMap = new HashMap<>();
        templateMap.put(CRMTopology.ELOQUA, "PLS/EloquaAndSalesforceToEloqua/Template_ELQ");
        templateMap.put(CRMTopology.MARKETO, "PLS/MarketoAndSalesforceToMarketo/Template_MKTO");
        templateMap.put(CRMTopology.SFDC, "PLS/Salesforce/Template_SFDC");
    }

    @Override
    public List<String> getOptions() {
        List<String> options = dirWatcher.getOptions();
        options.removeAll(Collections.singletonList(".svn"));
        return options;
    }

    public String getTemplate(String version, CRMTopology topology) {
        return absoluteRoot + "/" + version + "/" + templateMap.get(topology);
    }
}
