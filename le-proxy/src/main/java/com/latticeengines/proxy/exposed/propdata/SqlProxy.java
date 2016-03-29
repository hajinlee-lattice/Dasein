package com.latticeengines.proxy.exposed.propdata;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.propdata.ExportRequest;
import com.latticeengines.domain.exposed.propdata.ImportRequest;
import com.latticeengines.network.exposed.propdata.SqlInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component
public class SqlProxy extends BaseRestApiProxy implements SqlInterface {

    public SqlProxy() {
        super("propdata/sql");
    }

    @Override
    public AppSubmission importTable(ImportRequest importRequest) {
        String url = constructUrl("/imports");
        return post("import", url, importRequest, AppSubmission.class);
    }


    @Override
    public AppSubmission exportTable(ExportRequest exportRequest) {
        String url = constructUrl("/exports");
        return post("export", url, exportRequest, AppSubmission.class);
    }

}