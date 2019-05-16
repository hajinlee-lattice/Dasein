package com.latticeengines.proxy.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessRequest;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.domain.exposed.pls.FileProperty;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;

import io.micrometer.core.instrument.util.StringUtils;

@Component("dropBoxProxy")
public class DropBoxProxyImpl extends MicroserviceRestApiProxy implements DropBoxProxy {

    private static final Logger log = LoggerFactory.getLogger(DropBoxProxyImpl.class);

    protected DropBoxProxyImpl() {
        super("cdl");
    }

    @Override
    public DropBoxSummary getDropBox(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/dropbox/", //
                shortenCustomerSpace(customerSpace));
        return get("get drop box", url, DropBoxSummary.class);
    }

    @Override
    public GrantDropBoxAccessResponse grantAccess(String customerSpace, GrantDropBoxAccessRequest request) {
        String url = constructUrl("/customerspaces/{customerSpace}/dropbox/access", //
                shortenCustomerSpace(customerSpace));
        return put("grant drop box access", url, request, GrantDropBoxAccessResponse.class);
    }

    @Override
    public GrantDropBoxAccessResponse refreshAccessKey(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/dropbox/key", //
                shortenCustomerSpace(customerSpace));
        return put("refresh dropbox access key", url, null, GrantDropBoxAccessResponse.class);
    }

    @Override
    public GrantDropBoxAccessResponse getAccessKey(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/dropbox/key", //
                shortenCustomerSpace(customerSpace));
        return get("refresh dropbox access key", url, GrantDropBoxAccessResponse.class);
    }

    @Override
    public boolean createTemplateFolder(String customerSpace, String systemName, String objectName, String path) {
        if (StringUtils.isEmpty(objectName)) {// create default folder under system
            String url = "/customerspaces/{customerSpace}/dropbox/folder/{systemName}";
            url = constructUrl(url, customerSpace, formatFolder(systemName));
            return get("create template folder", url, Boolean.class);
        }
        String url = "/customerspaces/{customerSpace}/dropbox/folder/{objectName}";
        if (StringUtils.isNotEmpty(path)) {
            url = String.format("%s?path=%s", url, formatFolder(path));
            if (StringUtils.isNotEmpty(systemName))
                url = String.format("%s&systemName=%s", url, systemName);
        } else {
            if (StringUtils.isNotEmpty(systemName))
                url = String.format("%s?systemName=%s", url, systemName);
        }
        url = constructUrl(url, customerSpace, formatFolder(objectName));

        return post("create template folder", url, null, Boolean.class);
    }

    @Override
    public List<String> getAllSubFolders(String customerSpace, String systemName, String objectName, String path) {
        String url = "/customerspaces/{customerSpace}/dropbox/folder";
        if (StringUtils.isNotEmpty(objectName)) {
            if (StringUtils.isEmpty(path)) {
                url = String.format("%s?objectName=%s", url, formatFolder(objectName));
            } else {
                url = String.format("%s?objectName=%s&path=%s", url, formatFolder(objectName), formatFolder(path));
            }
            if (StringUtils.isNotEmpty(systemName))
                url = String.format("%s&systemName=%s", url, systemName);
        } else {
            if (StringUtils.isNotEmpty(systemName))
                url = String.format("%s?systemName=%s", url, systemName);
        }
        url = constructUrl(url, customerSpace);

        return getList("get all sub folders", url, String.class);
    }

    @Override
    public boolean importS3file(String customerSpace, String s3Path, String hdfsPath, String filename) {
        String url = "/customerspaces/{customerSpace}/dropbox/import?s3Path={s3Path}&hdfsPath={hdfsPath}&filename={filename}";
        url = constructUrl(url, customerSpace, s3Path, hdfsPath, filename);
        return post("Import File to S3", url, null, Boolean.class);
    }

    @Override
    public List<FileProperty> getFileListForPath(String customerSpace, String s3Path, String filter) {
        String url = "/customerspaces/{customerSpace}/dropbox/fileList?s3Path={s3Path}&filter={filter}";
        url = constructUrl(url, customerSpace, s3Path, filter);
        List<?> results = get("get all files under s3Path", url, List.class);
        return JsonUtils.convertList(results, FileProperty.class);
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
