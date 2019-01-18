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
import com.latticeengines.metadata.service.DataUnitRuntimeService;
import com.latticeengines.metadata.service.DataUnitService;

import io.swagger.annotations.Api;

@Api(value = "DataUnit", description = "REST resource for data units")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/dataunit")
public class DataUnitResource {

    private static final Logger log = LoggerFactory.getLogger(DataUnitResource.class);

    @Inject
    private DataUnitService dataUnitService;

    @PostMapping("")
    public DataUnit create(@PathVariable String customerSpace, @RequestBody DataUnit dataUnit) {

        return dataUnitService.createOrUpdateByNameAndStorageType(dataUnit);
    }

    @PutMapping("/delete")
    public Boolean delete(@PathVariable String customerSpace, @RequestBody DataUnit dataUnit) {
        DataUnitRuntimeService dataUnitRuntimeService = DataUnitRuntimeService.getRunTimeService(dataUnit.getClass());
        if (dataUnitRuntimeService == null) {
            throw new RuntimeException(
                    String.format("Cannot find the dataUnit runtime service for dataUnit class: %s",
                            dataUnit.getClass()));
        }
        return dataUnitRuntimeService.delete(dataUnit);
    }

    @PostMapping("/renameRedShiftTableName")
    public Boolean renameRedShiftTableName(@PathVariable String customerSpace, @RequestBody DataUnit dataUnit,
                                            @RequestParam(name="tableName") String tableName) {
        DataUnitRuntimeService dataUnitRuntimeService = DataUnitRuntimeService.getRunTimeService(dataUnit.getClass());
        if (dataUnitRuntimeService == null) {
            throw new RuntimeException(
                    String.format("Cannot find the dataUnit runtime service for dataUnit class: %s",
                            dataUnit.getClass()));
        }
        return dataUnitRuntimeService.renameTableName(dataUnit, tableName);
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
