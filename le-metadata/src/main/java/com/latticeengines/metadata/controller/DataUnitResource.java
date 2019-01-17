package com.latticeengines.metadata.controller;


import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.metadata.service.DataUnitService;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;

import io.swagger.annotations.Api;

@Api(value = "DataUnit", description = "REST resource for data units")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/dataunit")
public class DataUnitResource {

    private static final Logger log = LoggerFactory.getLogger(DataUnitResource.class);

    @Inject
    private DataUnitService dataUnitService;

    @Inject
    private RedshiftService redshiftService;

    @PostMapping("")
    public DataUnit create(@PathVariable String customerSpace, @RequestBody DataUnit dataUnit) {
        return dataUnitService.createOrUpdateByNameAndStorageType(dataUnit);
    }

    @PutMapping("")
    public Boolean delete(@PathVariable String customerSpace, @RequestBody DataUnit dataUnit) {
        log.info("delete RedshiftTable " + dataUnit.getName());
        redshiftService.dropTable(dataUnit.getName());
        log.info("delete dataUnit record : tenant is " + dataUnit.getTenant() + ", name is " + dataUnit.getName());
        dataUnitService.deleteByNameAndStorageType(dataUnit.getName(), dataUnit.getStorageType());
        return Boolean.TRUE;
    }

    @PostMapping("/renameRedShiftTableName")
    public DataUnit renameRedShiftTableName(@PathVariable String customerSpace, @RequestBody DataUnit dataUnit,
                                            @RequestParam(name="tableName") String tableName) {
        String originTableName = dataUnit.getName();
        log.info("rename RedShift tableName " + originTableName + " to " + tableName + " under tenant " + dataUnit.getTenant());
        DataUnit renameDataUnit = dataUnitService.renameRedShiftTableName(dataUnit, tableName);
        redshiftService.renameTable(originTableName, renameDataUnit.getName());
        return renameDataUnit;
    }

    @GetMapping("/type/{type}")
    public List<DataUnit> getByStorageType(@PathVariable String customerSpace,
                                  @PathVariable String type) {
        return dataUnitService.findAllByType(DataUnit.StorageType.valueOf(type));
    }

    @GetMapping("/name/{name}")
    public List<DataUnit> getDataUnit(@PathVariable String customerSpace, @PathVariable String name,
                                      @RequestParam(name = "type", required = false) DataUnit.StorageType storageType) {
        if (storageType == null) {
            return dataUnitService.findByNameFromReader(name);
        } else {
            DataUnit unit = dataUnitService.findByNameTypeFromReader(name, storageType);
            if (unit != null) {
                return Collections.singletonList(unit);
            } else {
                return Collections.emptyList();
            }
        }
    }

}
