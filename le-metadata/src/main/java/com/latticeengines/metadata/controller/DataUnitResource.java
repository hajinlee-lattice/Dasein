package com.latticeengines.metadata.controller;


import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.metadata.service.DataUnitService;

import io.swagger.annotations.Api;

@Api(value = "DataUnit", description = "REST resource for data units")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/dataunit")
public class DataUnitResource {

    @Inject
    private DataUnitService dataUnitService;

    @PostMapping("")
    public DataUnit create(@PathVariable String customerSpace, @RequestBody DataUnit dataUnit) {
        return dataUnitService.createOrUpdateByNameAndStorageType(dataUnit);
    }

    @GetMapping("")
    public List<DataUnit> findAllByType(@PathVariable String customerSpace,
                                  @RequestParam(name = "type") DataUnit.StorageType storageType) {
        return dataUnitService.findAllByType(storageType);
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
