package com.latticeengines.apps.cdl.controller;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.core.MediaType;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.DataCollectionManagerService;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLDataSpace;
import com.latticeengines.domain.exposed.cdl.ImportTemplateDiagnostic;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatusHistory;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

/**
 * This controller assumes operation on the default datacollection
 */
@Api(value = "datacollection", description = "REST resource for default metadata data collection")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/datacollection")
public class DataCollectionResource {

    @Inject
    private DataCollectionManagerService collectionMgrSvc;

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private SegmentService segmentService;

    @GetMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "Get the default data collection")
    public DataCollection getDataCollection(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataCollectionService.getDataCollection(customerSpace, null);
    }

    @PutMapping(value = "/version/{version}")
    @ResponseBody
    @ApiOperation(value = "Switch the version of default data collection")
    public ResponseDocument<DataCollection.Version> switchVersion(@PathVariable String customerSpace,
            @PathVariable DataCollection.Version version) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        DataCollection.Version version1 = dataCollectionService.switchDataCollectionVersion(customerSpace, null,
                version);
        return ResponseDocument.successResponse(version1);
    }

    @GetMapping(value = "/status")
    @ResponseBody
    @ApiOperation(value = "Get or create data collection status")
    public DataCollectionStatus getDataCollectionStatus(@PathVariable String customerSpace,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataCollectionService.getOrCreateDataCollectionStatus(customerSpace, version);
    }

    @PostMapping(value = "/version/{version}/status")
    @ResponseBody
    @ApiOperation(value = "Save or update data collection status")
    public void saveDataCollectionStatus(@PathVariable String customerSpace,
            @PathVariable DataCollection.Version version, @RequestBody DataCollectionStatus status) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataCollectionService.saveOrUpdateStatus(customerSpace, status, version);
    }

    @PostMapping(value = "/statushistory")
    @ResponseBody
    @ApiOperation(value = "Save data collection status history")
    public void saveDataCollectionStatusHistory(@PathVariable String customerSpace,
            @RequestBody DataCollectionStatus status) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataCollectionService.saveStatusHistory(customerSpace, status);
    }

    @GetMapping(value = "/statushistory")
    @ResponseBody
    @ApiOperation(value = "Get data collection status history")
    public List<DataCollectionStatusHistory> getDataCollectionStatusHistory(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataCollectionService.getCollectionStatusHistory(customerSpace);
    }

    @PutMapping(value = "/datacloudbuildnumber/{dataCloudBuildNumber:.+}")
    @ResponseBody
    @ApiOperation(value = "Switch the version of default data collection")
    public ResponseDocument<String> updateDataCloudVersion(@PathVariable String customerSpace,
            @PathVariable("dataCloudBuildNumber") String dataCloudBuildNumber) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        String newDataCloudVersion = dataCollectionService.updateDataCloudBuildNumber(customerSpace, null,
                dataCloudBuildNumber);
        return ResponseDocument.successResponse(newDataCloudVersion);
    }

    @GetMapping(value = "/tables")
    @ResponseBody
    @ApiOperation(value = "Get the first table based on table role")
    public Table getTable(@PathVariable String customerSpace, @RequestParam(value = "role") TableRoleInCollection role,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        List<Table> tables = dataCollectionService.getTables(customerSpace, null, role, version);
        if (tables == null || tables.isEmpty()) {
            return null;
        } else {
            return tables.get(0);
        }
    }

    @GetMapping(value = "/alltables")
    @ResponseBody
    @ApiOperation(value = "Get all tables based on table role")
    public List<Table> getTables(@PathVariable String customerSpace,
            @RequestParam(value = "role") TableRoleInCollection role,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        List<Table> tables = dataCollectionService.getTables(customerSpace, null, role, version);
        if (tables == null || tables.isEmpty()) {
            return null;
        } else {
            return tables;
        }
    }

    @GetMapping(value = "/tablenames")
    @ResponseBody
    @ApiOperation(value = "Get the Table names via table role and version")
    public List<String> getTableNames(@PathVariable String customerSpace,
            @RequestParam(value = "role", required = false) TableRoleInCollection role,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataCollectionService.getTableNames(customerSpace, null, role, version);
    }

    @GetMapping(value = "/tablenames/signatures")
    @ResponseBody
    @ApiOperation(value = "Get a map of signature to tableName via table role, version and list of signatures")
    public Map<String, String> getTableNames(@PathVariable String customerSpace,
            @RequestParam(value = "signatures") List<String> signatures,
            @RequestParam(value = "role", required = false) TableRoleInCollection role,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataCollectionService.getTableNamesWithSignatures(customerSpace, null, role, version,
                signatures == null ? null : new HashSet<>(signatures));
    }

    @GetMapping(value = "/segments")
    @ResponseBody
    @ApiOperation(value = "Get the all segments in the default collection.")
    public List<MetadataSegment> getSegments(@PathVariable String customerSpace) {
        return segmentService.getSegments();
    }

    @GetMapping(value = "/stats")
    @ResponseBody
    @ApiOperation(value = "Get the main statistics of the default collection.")
    public StatisticsContainer getMainStats(@PathVariable String customerSpace,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        StatisticsContainer container = dataCollectionService.getStats(customerSpace, null, version);
        return container == null ? null : container.detachHibernate();
    }

    @GetMapping(value = "/attrrepo")
    @ResponseBody
    @ApiOperation(value = "Get the attribute repository of the default collection.")
    public AttributeRepository getAttrRepo(@PathVariable String customerSpace,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataCollectionService.getAttrRepo(customerSpace, null, version);
    }

    @PostMapping(value = "/tables/{tableName}")
    @ResponseBody
    @ApiOperation(value = "Create or insert a table into the collection")
    public SimpleBooleanResponse upsertTable(@PathVariable String customerSpace, //
            @PathVariable String tableName, //
            @RequestParam(value = "role") TableRoleInCollection role,
            @RequestParam(value = "version") DataCollection.Version version) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataCollectionService.upsertTable(customerSpace, null, tableName, role, version);
        return SimpleBooleanResponse.successResponse();
    }

    @PostMapping(value = "/tables/multi/{tableNames}")
    @ResponseBody
    @ApiOperation(value = "Create or insert tables into the collection")
    public SimpleBooleanResponse upsertTables(@PathVariable String customerSpace, //
            @PathVariable String[] tableNames, //
            @RequestParam(value = "role") TableRoleInCollection role,
            @RequestParam(value = "version") DataCollection.Version version) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataCollectionService.upsertTables(customerSpace, null, tableNames, role, version);
        return SimpleBooleanResponse.successResponse();
    }

    @PostMapping(value = "/tables/signatures")
    @ResponseBody
    @ApiOperation(value = "Create or insert tables with signatures into the collection")
    public SimpleBooleanResponse upsertTablesWithSignatures(@PathVariable String customerSpace, //
            @RequestBody Map<String, String> signatureTableMap, // signature -> tableName
            @RequestParam(value = "role") TableRoleInCollection role, //
            @RequestParam(value = "version") DataCollection.Version version) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataCollectionService.upsertTables(customerSpace, null, signatureTableMap, role, version);
        return SimpleBooleanResponse.successResponse();
    }

    @DeleteMapping(value = "/tables/{tableName}")
    @ResponseBody
    @ApiOperation(value = "Create or insert a table into the collection")
    public SimpleBooleanResponse removeTable(@PathVariable String customerSpace, //
            @PathVariable String tableName, //
            @RequestParam(value = "role") TableRoleInCollection role,
            @RequestParam(value = "version") DataCollection.Version version) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataCollectionService.removeTable(customerSpace, null, tableName, role, version);
        return SimpleBooleanResponse.successResponse();
    }

    @DeleteMapping(value = "/tables")
    @ResponseBody
    @ApiOperation(value = "Unlink all tables under tenant in current active data collection")
    public SimpleBooleanResponse unlinkTables(@PathVariable String customerSpace, //
                                             @RequestParam(value = "role", required = false) TableRoleInCollection role,
                                             @RequestParam(value = "version") DataCollection.Version version) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        if (role == null) {
            dataCollectionService.unlinkTables(customerSpace, version);
        } else {
            dataCollectionService.unlinkTables(customerSpace, null, role, version);
        }
        return SimpleBooleanResponse.successResponse();
    }

    @PostMapping(value = "/resettables")
    @ResponseBody
    @ApiOperation(value = "Create or insert a table into the collection")
    public SimpleBooleanResponse resetTable(@PathVariable String customerSpace, //
            @RequestParam(value = "role") TableRoleInCollection role) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataCollectionService.resetTable(customerSpace, null, role);
        return SimpleBooleanResponse.successResponse();
    }

    @PostMapping(value = "/stats")
    @ResponseBody
    @ApiOperation(value = "Create or update the main statistics of the collection")
    public SimpleBooleanResponse upsertStats(@PathVariable String customerSpace, //
            @RequestBody StatisticsContainer statisticsContainer) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataCollectionService.addStats(customerSpace, null, statisticsContainer);
        return SimpleBooleanResponse.successResponse();
    }

    @DeleteMapping(value = "/stats")
    @ResponseBody
    @ApiOperation(value = "Remove the main statistics of the collection")
    public SimpleBooleanResponse removeStats(@PathVariable String customerSpace,
            @RequestParam(value = "version") DataCollection.Version version) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataCollectionService.removeStats(customerSpace, null, version);
        return SimpleBooleanResponse.successResponse();
    }

    @GetMapping(value = "/attributegroups")
    @ResponseBody
    @ApiOperation(value = "Get mocked metadat of attribute group for company profile and talking point")
    public List<String> getAttributeGroupsForCompanyProfileAndTalkingPoints(@PathVariable String customerSpace) {
        return Arrays.asList(InterfaceName.CompanyName.toString(), InterfaceName.City.toString(),
                InterfaceName.Country.toString(), InterfaceName.Industry.toString(), InterfaceName.Website.toString(),
                InterfaceName.YearStarted.toString());
    }

    @PostMapping(value = "/reset")
    @ResponseBody
    @ApiOperation(value = "Reset the full data collection or an business entity")
    public ResponseDocument<String> reset(@PathVariable String customerSpace,
            @RequestParam(value = "entity", required = false) BusinessEntity entity) {
        String customerSpaceString = CustomerSpace.parse(customerSpace).toString();
        boolean status;
        if (entity == null) {
            status = collectionMgrSvc.resetAll(customerSpaceString);
        } else {
            status = collectionMgrSvc.resetEntity(customerSpaceString, entity);
        }
        if (status) {
            return ResponseDocument.successResponse("Success");
        } else {
            return ResponseDocument.failedResponse(new RuntimeException("Failed to reset"));
        }
    }

    @DeleteMapping(value = "/clearcache")
    @ResponseBody
    @ApiOperation(value = "Clear cache for data collection")
    public ResponseDocument<String> clearCache(@PathVariable String customerSpace) {
        dataCollectionService.clearCache(customerSpace);
        return ResponseDocument.successResponse("Success");
    }

    @PutMapping(value = "/refreshcounts")
    @ResponseBody
    @ApiOperation(value = "Refresh segment and rating engine counts.")
    public SimpleBooleanResponse refreshCount(@PathVariable String customerSpace) {
        collectionMgrSvc.refreshCounts(customerSpace);
        return SimpleBooleanResponse.successResponse();
    }

    @GetMapping(value = "/dataspace")
    @ResponseBody
    @ApiOperation(value = "Dump out the paths of tenant's CDL data space")
    public CDLDataSpace getCDLDataSpace(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataCollectionService.createCDLDataSpace(customerSpace);
    }

    @GetMapping(value = "/artifact")
    @ApiOperation(value = "Get data collection artifacts by customer space, artifact status and version.")
    public List<DataCollectionArtifact> getArtifacts(@PathVariable String customerSpace,
            @RequestParam(value = "status", required = false) DataCollectionArtifact.Status status,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataCollectionService.getArtifacts(customerSpace, status, version);
    }

    @GetMapping(value = "/artifact/{name}")
    @ApiOperation(value = "Get single data collection artifact by customer space, artifact name and version.")
    public DataCollectionArtifact getArtifact(@PathVariable String customerSpace, @PathVariable String name,
            @RequestParam(value = "version", required = false) DataCollection.Version version,
            @RequestParam(value = "latest", required = false, defaultValue = "true") Boolean getLatest) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        if (getLatest == null) {
            getLatest = true;
        }
        if (getLatest) {
            return dataCollectionService.getLatestArtifact(customerSpace, name, version);
        } else {
            return dataCollectionService.getOldestArtifact(customerSpace, name, version);
        }
    }

    @PutMapping(value = "/artifact")
    @ApiOperation(value = "Update a data collection artifact.")
    public DataCollectionArtifact updateArtifact(@PathVariable String customerSpace,
            @RequestBody DataCollectionArtifact artifact) {
        if (artifact == null) {
            return null;
        }

        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataCollectionService.updateArtifact(customerSpace, artifact);
    }

    @PostMapping(value = "/artifact/version/{version}")
    @ApiOperation(value = "Create a data collection artifact.")
    public DataCollectionArtifact createArtifact(@PathVariable String customerSpace,
            @PathVariable DataCollection.Version version, @RequestBody DataCollectionArtifact artifact) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataCollectionService.createArtifact(customerSpace, artifact.getName(), artifact.getUrl(),
                artifact.getStatus(), version);
    }

    @DeleteMapping(value = "/artifact/{name}")
    @ApiOperation(value = "Delete a data collection artifact.")
    public DataCollectionArtifact deleteArtifact(@PathVariable String customerSpace, @PathVariable String name,
            @RequestParam(value = "version") DataCollection.Version version,
            @RequestParam(value = "latest", required = false, defaultValue = "true") Boolean deleteLatest) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        if (deleteLatest == null) {
            deleteLatest = true;
        }
        return dataCollectionService.deleteArtifact(customerSpace, name, version, deleteLatest);
    }

    @GetMapping(value = "/artifact/{exportId}/download", produces = MediaType.APPLICATION_OCTET_STREAM)
    @ApiOperation(value = "Download a data collection artifact")
    public byte[] downloadArtifact(@PathVariable String customerSpace, @PathVariable String exportId) {
        return dataCollectionService.downloadDataCollectionArtifact(customerSpace, exportId);
    }

    @PostMapping(value = "/diagnostic/{dataCollectionTablePid}")
    @ResponseBody
    @ApiOperation(value = "diagnose collection and service store table")
    public ResponseDocument<ImportTemplateDiagnostic> servingAndBatchDiagnostic(@PathVariable String customerSpace, @PathVariable Long dataCollectionTablePid) {
        return ResponseDocument.successResponse(dataCollectionService.diagnostic(customerSpace, dataCollectionTablePid));
    }
}
