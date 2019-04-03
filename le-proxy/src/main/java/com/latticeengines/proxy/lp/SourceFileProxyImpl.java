package com.latticeengines.proxy.lp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.CopySourceFileRequest;
import com.latticeengines.domain.exposed.pls.FileProperty;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.lp.SourceFileProxy;

@Component
public class SourceFileProxyImpl extends MicroserviceRestApiProxy implements SourceFileProxy {

    private static final String URL_PRERIX = "/customerspaces/{customerSpace}/sourcefiles";

    protected SourceFileProxyImpl() {
        super("lp");
    }

    @Override
    public SourceFile findByName(String customerSpace, String name) {
        String url = URL_PRERIX + "/?name={name}";
        url = constructUrl(url, shortenCustomerSpace(customerSpace), name);
        return get("get SourceFile by name", url, SourceFile.class);
    }

    @Override
    public SourceFile findByTableName(String customerSpace, String tableName) {
        String url = URL_PRERIX + "/tablename/{tableName}";
        url = constructUrl(url, shortenCustomerSpace(customerSpace), tableName);
        return get("get SourceFile by table name", url, SourceFile.class);
    }

    @Override
    public SourceFile findByApplicationId(String customerSpace, String applicationId) {
        String url = URL_PRERIX + "/applicationid/{applicationId}";
        url = constructUrl(url, shortenCustomerSpace(customerSpace), applicationId);
        return get("get SourceFile by applicationId", url, SourceFile.class);
    }

    @Override
    public void create(String customerSpace, SourceFile sourceFile) {
        String url = constructUrl(URL_PRERIX, shortenCustomerSpace(customerSpace));
        post("create SourceFile", url, sourceFile, Object.class);
    }

    @Override
    public void update(String customerSpace, SourceFile sourceFile) {
        String url = constructUrl(URL_PRERIX, shortenCustomerSpace(customerSpace));
        put("update SourceFile", url, sourceFile, Object.class);
    }

    @Override
    public void delete(String customerSpace, String sourceFileName) {
        String url = URL_PRERIX + "/name/{sourceFileName}";
        url = constructUrl(url, shortenCustomerSpace(customerSpace), sourceFileName);
        super.delete("get SourceFile by table name", url);
    }

    @Override
    public void copySourceFile(String customerSpace, String originalSourceFileName, String targetTableName, String targetTenant) {
        CopySourceFileRequest request = new CopySourceFileRequest();
        request.setOriginalSourceFile(originalSourceFileName);
        request.setTargetTable(targetTableName);
        request.setTargetTenant(shortenCustomerSpace(targetTenant));
        String url = constructUrl(URL_PRERIX + "/copy", shortenCustomerSpace(customerSpace));
        post("copy SourceFile", url, request);
    }

    @Override
    public SourceFile findByTableNameCrossTenant(String tableName) {
        String url = "/sourcefiles/tablename/{tableName}";
        url = constructUrl(url, tableName);
        return get("get SourceFile by table name cross tenant", url, SourceFile.class);
    }

    @Override
    public SourceFile createSourceFileFromS3(String customerSpace, FileProperty fileProperty,
                                             SchemaInterpretation schemaInterpretation, String entity) {
        String url = URL_PRERIX + "/fromS3?schema={schema}&entity={entity}";
        url = constructUrl(url, shortenCustomerSpace(customerSpace), schemaInterpretation, entity);
        return post("create SourceFile by s#", url, fileProperty, SourceFile.class);
    }

}
