package com.latticeengines.metadata.controller;


import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.datastore.AthenaDataUnit;
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
    public DataUnit create(@PathVariable String customerSpace,
                           @RequestParam(value = "purgeOldSnapShot", defaultValue = "true", required = false) boolean purgeOldSnapShot, @RequestBody DataUnit dataUnit) {
        return dataUnitService.createOrUpdateByNameAndStorageType(dataUnit, purgeOldSnapShot);
    }

    @PutMapping("")
    public DataUnit updateByNameAndType(@PathVariable String customerSpace,
                                        @RequestParam(value = "purgeOldSnapShot", defaultValue = "true", required = false) boolean purgeOldSnapShot, @RequestBody DataUnit dataUnit) {
        return dataUnitService.createOrUpdateByNameAndStorageType(dataUnit, purgeOldSnapShot);
    }

    @PutMapping("/delete")
    public Boolean delete(@PathVariable String customerSpace, @RequestBody DataUnit dataUnit) {
        return dataUnitService.delete(dataUnit);
    }

    @DeleteMapping("/delete/name/{name}")
    public Boolean deleteByNameAndType(@PathVariable String customerSpace, @PathVariable String name,
                                       @RequestParam(name = "type") DataUnit.StorageType storageType) {
        return dataUnitService.delete(name, storageType);
    }

    @PostMapping("/updateSignature")
    public void updateSignature(@PathVariable String customerSpace, @RequestBody DataUnit dataUnit,
                                @RequestParam(name = "signature") String signature) {
        dataUnitService.updateSignature(dataUnit, signature);
    }

    @PostMapping("/renameTableName")
    public Boolean renameTableName(@PathVariable String customerSpace, @RequestBody DataUnit dataUnit,
                                            @RequestParam(name="tableName") String tableName) {
        return dataUnitService.renameTableName(dataUnit, tableName);
    }

    @GetMapping("/type/{type}")
    public List<DataUnit> getByStorageType(@PathVariable String customerSpace,
                                  @PathVariable String type) {
        return dataUnitService.findAllByType(DataUnit.StorageType.valueOf(type));
    }

    @GetMapping("/template/{templateId}")
    public DataUnit getByDataTemplateIdAndRole(@PathVariable String customerSpace, @PathVariable String templateId,
                                               @RequestParam(name = "role") DataUnit.Role role) {
        return dataUnitService.findByDataTemplateIdAndRole(templateId, role);
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

    @PostMapping("/name/{name}/athena-unit")
    public AthenaDataUnit registerAthenaDataUnit(@PathVariable String customerSpace, @PathVariable String name) {
        return dataUnitService.registerAthenaDataUnit(name);
    }

}
