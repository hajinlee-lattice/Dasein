package com.latticeengines.proxy.exposed.core;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

// TODO(yintao): Do I need to add "implements ProxyInterface"?
@Component("importWorkflowSpecProxy")
public class ImportWorkflowSpecProxy extends MicroserviceRestApiProxy  {

    // TODO(yintao): Should I have this function?
    protected ImportWorkflowSpecProxy() {
        super("cdl");
    }

    public ImportWorkflowSpecProxy(String hostProxy) {
        super(hostProxy, "cdl");
    }

    public ImportWorkflowSpec getImportWorkflowSpec(String customerSpace, String systemType, String systemObject) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/importworkflowspec" +
                        "?systemType={systemType}&systemObject={systemObject}",
                shortenCustomerSpace(customerSpace), systemType, systemObject);
        return get("get import workflow spec", url, ImportWorkflowSpec.class);
    }

    public List<ImportWorkflowSpec> getSpecsByTypeAndObject(String customerSpace, String systemType,
                                                            String systemObject, String excludeSystemType) {
        String url = constructUrl("/customerspaces/{customerSpace}/importworkflowspec/list",
                shortenCustomerSpace(customerSpace));
        List<String> params = new ArrayList<>();
        if (systemType != null) {
            params.add("systemType=" + systemType);
        }
        if (systemObject != null) {
            params.add("systemObject=" + systemObject);
        }
        if (excludeSystemType != null) {
            params.add("excludeSystemType=" + excludeSystemType);
        }
        if (!params.isEmpty()) {
            url += "?" + StringUtils.join(params, "&");
        }
        return getList("get import workflow spec by type and object", url, ImportWorkflowSpec.class);
    }

    public Table generateTable(String customerSpace, String tableName, Boolean writeAll,
                               FieldDefinitionsRecord record) {
        String url = constructUrl("/customerspaces/{customerSpace}/importworkflowspec/table",
                shortenCustomerSpace(customerSpace));
        List<String> params = new ArrayList<>();
        if (StringUtils.isNotBlank(tableName)) {
            params.add("tableName=" + tableName);
        }
        if (writeAll != null) {
            params.add("writeAll=" + writeAll.toString());
        }
        if (!params.isEmpty()) {
            url += "?" + StringUtils.join(params, "&");
        }
        return post("generateTable", url, record, Table.class);
    }

    public void addSpecToS3(String customerSpace, String systemType, String systemObject, ImportWorkflowSpec spec) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/importworkflowspec" +
                        "?systemType={systemType}&systemObject={systemObject}",
                shortenCustomerSpace(customerSpace), systemType, systemObject);
        post("putSpecToS3", url, spec, Void.class);
    }

    public void deleteSpecFromS3(String customerSpace, String systemType, String systemObject) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/importworkflowspec" +
                        "?systemType={systemType}&systemObject={systemObject}",
                shortenCustomerSpace(customerSpace), systemType, systemObject);
        delete("deleteSpecFromS3", url);
    }
}
