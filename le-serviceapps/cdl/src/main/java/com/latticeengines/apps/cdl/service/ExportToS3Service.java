package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public interface ExportToS3Service {

    void buildRequests(CustomerSpace customerSpace, List<ExportRequest> requests);

    void executeRequests(List<ExportRequest> requests);

    void buildDataUnits(List<ExportRequest> requests);

    public static class ExportRequest {
        public String srcDir;
        public String tgtDir;
        public CustomerSpace customerSpace;

        public ExportRequest() {
        }

        public ExportRequest(String srcDir, String tgtDir, CustomerSpace customerSpace) {
            this.srcDir = srcDir;
            this.tgtDir = tgtDir;
            this.customerSpace = customerSpace;
        }
    }

}
