package com.latticeengines.proxy.cdl;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.cdl.DropFolderProxy;

@Component("dropFolderProxy")
public class DropFolderProxyImpl extends MicroserviceRestApiProxy implements DropFolderProxy {
    private static final Logger log = LoggerFactory.getLogger(DropFolderProxyImpl.class);

    protected DropFolderProxyImpl() {
        super("cdl");
    }

    @Override
    public boolean createTemplateFolder(String customerSpace, String objectName, String path) {
        String url = "/customerspaces/{customerSpace}/dropfolder/{objectName}";
        if (StringUtils.isNotEmpty(path)) {
            url = String.format("%s?path=%s", url, formatFolder(path));
        }
        url = constructUrl(url, customerSpace, formatFolder(objectName));

        return post("create template folder", url,null, Boolean.class);
    }

    @Override
    public List<String> getAllSubFolders(String customerSpace, String objectName, String path) {
        String url = "/customerspaces/{customerSpace}/dropfolder";
        if (StringUtils.isNotEmpty(objectName)) {
            if (StringUtils.isEmpty(path)) {
                url = String.format("%s?objectName=%s", url, formatFolder(objectName));
            } else {
                url = String.format("%s?objectName=%s&path=%s", url, formatFolder(objectName), formatFolder(path));
            }
        }
        url = constructUrl(url, customerSpace);

        return getList("get all sub folders", url, String.class);
    }

    private String formatFolder(String folder) {
        if (StringUtils.isNotEmpty(folder)) {
            if (folder.startsWith("/")) {
                folder = folder.substring(1);
            }
            if (folder.endsWith("/")) {
                folder = folder.substring(0, folder.length() - 1);
            }
        }

        return folder;
    }
}
