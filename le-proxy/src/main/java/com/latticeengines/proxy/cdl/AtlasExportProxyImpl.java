package com.latticeengines.proxy.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
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
    public void updateAtlasExportStatus(String customerSpace, String uuid, MetadataSegmentExport.Status status) {
        String url = constructUrl("/customerspaces/{customerSpace}/atlas/export?uuid={uuid}&status={status}", //
                shortenCustomerSpace(customerSpace), uuid, status);
        put("update atlas export", url);
    }

    @Override
    public void addFileToSystemPath(String customerSpace, String uuid, String fileName, List<String> pathToDelete) {
        String url = constructUrl("/customerspaces/{customerSpace}/atlas/export/systemfiles?uuid={uuid}&fileName={fileName}", //
                shortenCustomerSpace(customerSpace), uuid, fileName);
        post("Add export fileName with system path", url, pathToDelete, Void.class);
    }

    @Override
    public void addFileToDropFolder(String customerSpace, String uuid, String fileName, List<String> pathToDelete) {
        String url = constructUrl("/customerspaces/{customerSpace}/atlas/export/dropfolderfiles?uuid={uuid}&fileName={fileName}", //
                shortenCustomerSpace(customerSpace), uuid, fileName);
        post("Add export fileName with dropfolder", url, pathToDelete, Void.class);
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

    @Override
    public List<AtlasExport> findAll(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/atlas/export/findAll", //
                shortenCustomerSpace(customerSpace));
        List<?> rawlist = get("find all atlas exports", url, List.class);
        return JsonUtils.convertList(rawlist, AtlasExport.class);
    }

    @Override
    public AtlasExport createAtlasExport(String customerSpace, AtlasExport atlasExport) {
        String url = constructUrl("/customerspaces/{customerSpace}/atlas/export", //
                shortenCustomerSpace(customerSpace));
        return post("create atlas export", url, atlasExport, AtlasExport.class);
    }
}
