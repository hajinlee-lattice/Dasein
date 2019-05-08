package com.latticeengines.proxy.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.cdl.AtlasExportProxy;

@Component("atlasExportProxy")
public class AtlasExportProxyImpl extends MicroserviceRestApiProxy implements AtlasExportProxy {

    protected AtlasExportProxyImpl() {
        super("cdl");
    }

    @Override
    public AtlasExport findAtlasExportById(String customerSpace, String uuid) {
        String url = constructUrl("/customerspaces/{customerSpace}/atlas/export?uuid={uuid}", //
                shortenCustomerSpace(customerSpace), uuid);
        return get("find atlas export by id", url, AtlasExport.class);
    }

    @Override
    public void addFileToSystemPath(String customerSpace, String uuid, String fileName) {
        String url = constructUrl("/customerspaces/{customerSpace}/atlas/export/systemfiles?uuid={uuid}&fileName={fileName}", //
                shortenCustomerSpace(customerSpace), uuid, fileName);
        post("Add export fileName with system path", url, Void.class);
    }

    @Override
    public void addFileToDropFolder(String customerSpace, String uuid, String fileName) {
        String url = constructUrl("/customerspaces/{customerSpace}/atlas/export/dropfolderfiles?uuid={uuid}&fileName={fileName}", //
                shortenCustomerSpace(customerSpace), uuid, fileName);
        post("Add export fileName with dropfolder", url, Void.class);
    }

    @Override
    public String getDropFolderExportPath(String customerSpace, AtlasExportType exportType, String datePrefix,
                                          boolean withProtocol) {
        String url = constructUrl("/customerspaces/{customerSpace}/atlas/export/dropfolder/path?" +
                        "exportType={exportType}&datePrefix={datePrefix}&withProtocol={withProtocol}", //
                shortenCustomerSpace(customerSpace), exportType, datePrefix, String.valueOf(withProtocol));
        return get("Get export dropfolder", url, String.class);
    }

    @Override
    public String getSystemExportPath(String customerSpace, boolean withProtocol) {
        String url = constructUrl("/customerspaces/{customerSpace}/atlas/export/system/path?" +
                        "withProtocol={withProtocol}", //
                shortenCustomerSpace(customerSpace), String.valueOf(withProtocol));
        return get("Get export dropfolder", url, String.class);
    }

    @Override
    public String getS3PathWithProtocol(String customerSpace, String relativePath) {
        String url = constructUrl("/customerspaces/{customerSpace}/atlas/export/s3/path", //
                shortenCustomerSpace(customerSpace));
        return post("Get s3 path with protocol", url, relativePath, String.class);
    }
}
