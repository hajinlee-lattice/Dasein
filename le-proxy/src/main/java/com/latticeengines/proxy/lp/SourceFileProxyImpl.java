package com.latticeengines.proxy.lp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.CopySourceFileRequest;
import com.latticeengines.domain.exposed.pls.FileProperty;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.lp.SourceFileProxy;

@Component
public class SourceFileProxyImpl extends MicroserviceRestApiProxy implements SourceFileProxy {

    private static final Logger log = LoggerFactory.getLogger(SourceFileProxyImpl.class);
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
    public SourceFile createSourceFileFromS3(String customerSpace, FileProperty fileProperty,  String entity) {
        String url = URL_PRERIX + "/fromS3?entity={entity}";
        url = constructUrl(url, shortenCustomerSpace(customerSpace), entity);
        ResponseDocument responseDocument = post("create SourceFile by s#", url, fileProperty, ResponseDocument.class);
        if (responseDocument == null) {
            return null;
        }
        if (responseDocument.isSuccess()) {
            return JsonUtils.convertValue(responseDocument.getResult(), SourceFile.class);
        }else {
            log.error(JsonUtils.serialize(responseDocument.getErrors()));
            throw new LedpException(LedpCode.LEDP_18053,
                    new String[] {JsonUtils.serialize(responseDocument.getErrors())});
        }
    }

}
