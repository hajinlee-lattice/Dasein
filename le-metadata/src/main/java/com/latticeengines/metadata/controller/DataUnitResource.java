package com.latticeengines.metadata.controller;


import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
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

}
