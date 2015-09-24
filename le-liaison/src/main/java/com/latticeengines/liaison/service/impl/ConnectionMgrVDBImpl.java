package com.latticeengines.liaison.service.impl;

import java.io.IOException;
import java.util.*;

import com.latticeengines.domain.exposed.dataloader.GetSpecRequest;
import com.latticeengines.domain.exposed.dataloader.GetSpecResult;
import com.latticeengines.domain.exposed.dataloader.InstallResult;
import com.latticeengines.domain.exposed.dataloader.InstallResult.ValueResult;
import com.latticeengines.domain.exposed.dataloader.InstallTemplateRequest;
import com.latticeengines.domain.exposed.dataplatform.visidb.GetQueryMetaDataColumnsRequest;
import com.latticeengines.domain.exposed.dataplatform.visidb.GetQueryMetaDataColumnsResponse;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata.AttributeMetadata;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata.KV;
import com.latticeengines.liaison.exposed.service.ConnectionMgr;
import com.latticeengines.liaison.exposed.service.Query;
import com.latticeengines.remote.exposed.service.DataLoaderService;

public class ConnectionMgrVDBImpl implements ConnectionMgr {

    private final String tenantName;
    private final String dlURL;

    private DataLoaderService dataLoaderService;

    public ConnectionMgrVDBImpl(String tenantName, String dlURL, DataLoaderService dataLoaderService) {
        this.tenantName = tenantName;
        this.dlURL = dlURL;
        this.dataLoaderService = dataLoaderService;
    }

    public Query getQuery(String queryName) throws IOException, RuntimeException {
        String spec = getSpec(queryName);
        return new QueryVDBImpl(queryName, spec);
    }

    public Map<String, Map<String, String>> getMetadata(String queryName) throws IOException, RuntimeException {

        final Set<String> allowedExtensions = new HashSet<String>(Arrays.asList("Category", "DataType"));

        GetQueryMetaDataColumnsResponse getMetadataResponse = dataLoaderService.getQueryMetadataColumns(new GetQueryMetaDataColumnsRequest(tenantName, queryName), dlURL);

        if (getMetadataResponse.getStatus() != 3) {
            throw new RuntimeException(String.format("Query \"%s\" not found for tenant \"%s\" at DataLoader URL %s", queryName, tenantName, dlURL));
        }

        List<AttributeMetadata> mdraw = getMetadataResponse.getMetadata();

        Map<String, Map<String, String>> columns = new HashMap<>();

        for (AttributeMetadata colData : mdraw) {
            Map<String, String> metadata = new HashMap<>();
            if (colData.getDisplayName() != null) {
                metadata.put("DisplayName", colData.getDisplayName());
            } else {
                metadata.put("DisplayName", "<NULL>");
            }

            if (colData.getDescription() != null) {
                metadata.put("Description", colData.getDescription());
            } else {
                metadata.put("Description", "<NULL>");
            }

            if (colData.getTags() != null && colData.getTags().size() > 0) {
                metadata.put("Tags", colData.getTags().get(colData.getTags().size() - 1));
            } else {
                metadata.put("Tags", "<NULL>");
            }

            if (colData.getFundamentalType() != null) {
                metadata.put("FundamentalType", colData.getFundamentalType());
            } else {
                metadata.put("FundamentalType", "<NULL>");
            }

            if (colData.getDisplayDiscretizationStrategy() != null) {
                metadata.put("DisplayDiscretizationStrategy", colData.getDisplayDiscretizationStrategy());
            } else {
                metadata.put("DisplayDiscretizationStrategy", "<NULL>");
            }

            if (colData.getExtensions() != null && colData.getExtensions().size() > 0) {
                for (KV kv : colData.getExtensions()) {
                    if (!allowedExtensions.contains(kv.getKey())) {
                        continue;
                    }
                    metadata.put(kv.getKey(), kv.getValue());
                }
            }
            if (!metadata.containsKey("Category")) {
                metadata.put("Category", "<NULL>");
            }
            if (!metadata.containsKey("DataType")) {
                metadata.put("DataType", "<NULL>");
            }

            // Map multiple values or no value to "Lattice Data Cloud"
            if (colData.getDataSource() != null && colData.getDataSource().size() == 1 && !colData.getDataSource().get(0).equals("")) {
                metadata.put("DataSource", colData.getDataSource().get(0));
            } else {
                metadata.put("DataSource", "Lattice Data Cloud");
            }

            if (colData.getApprovedUsage() != null && colData.getApprovedUsage().size() > 0) {
                metadata.put("ApprovedUsage", colData.getApprovedUsage().get(colData.getApprovedUsage().size() - 1));
            } else {
                metadata.put("ApprovedUsage", "<NULL>");
            }

            if (colData.getStatisticalType() != null) {
                metadata.put("StatisticalType", colData.getStatisticalType());
            } else {
                metadata.put("StatisticalType", "<NULL>");
            }

            columns.put(colData.getColumnName(), metadata);
        }
        return columns;
    }

    public void setQuery(Query query) throws IOException, RuntimeException {
        setSpec(query.getName(), "SpecLatticeNamedElements((" + query.definition() + "))");
    }

    public String getSpec(String specName) throws IOException, RuntimeException {

        GetSpecResult getSpecResult = dataLoaderService.getSpecDetails(new GetSpecRequest(tenantName, specName), dlURL);

        if (!getSpecResult.getSuccess().equalsIgnoreCase("true")) {
            if (getSpecResult.getErrorMessage().equals(String.format("Tenant \'%s\' does not exist.", tenantName))) {
                throw new RuntimeException(String.format("Tenant \"%s\" not found at DataLoader URL %s", tenantName, dlURL));
            }
            throw new RuntimeException(String.format("Tenant \"%s\" does not have spec \"%s\"", tenantName, specName));
        }

        return getSpecResult.getSpecDetails();
    }

    public void setSpec(String objName, String specLatticeNamedElements) throws IOException, RuntimeException {

        StringBuilder vfile = new StringBuilder(100000);

        vfile.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<VisiDBStructures appName=\"\">\n  <workspaces>\n    <workspace name=\"Workspace\">\n      <specs>\n");
        vfile.append(specLatticeNamedElements);
        vfile.append("\n      </specs>\n    </workspace>\n  </workspaces>\n</VisiDBStructures>");

        InstallResult getInstallResult = dataLoaderService.installVisiDBStructureFile(new InstallTemplateRequest(tenantName, vfile.toString()), dlURL);

        if (getInstallResult.getStatus() != 3) {
            throw new RuntimeException(String.format("Failed to set specs for tenant \"%s\" at DataLoader URL %s", tenantName, dlURL));
        }

        List<ValueResult> vrs = getInstallResult.getValueResult();
        ValueResult status2 = vrs.get(0);

        if (!status2.getValue().equals("Succeed")) {
            throw new RuntimeException(String.format("DataLoader error setting specs for tenant \"%s\" at DataLoader URL %s", tenantName, dlURL));
        }
    }

}
