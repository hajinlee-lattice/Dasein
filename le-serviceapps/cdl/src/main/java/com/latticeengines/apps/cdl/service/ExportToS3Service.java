package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public interface ExportToS3Service {

    void buildRequests(CustomerSpace customerSpace, List<ExportRequest> requests, boolean onlyAtlas);

    void executeRequests(List<ExportRequest> requests);

    void buildDataUnits(CustomerSpace customerSpace);

    class ExportRequest {
        public String name;
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

        public ExportRequest(String name, String srcDir, String tgtDir, CustomerSpace customerSpace) {
            this.name = name;
            this.srcDir = srcDir;
            this.tgtDir = tgtDir;
            this.customerSpace = customerSpace;
        }
    }

}
