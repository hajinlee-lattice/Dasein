package com.latticeengines.metadata.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.datastore.DataTemplate;
import com.latticeengines.metadata.service.DataTemplateService;

import io.swagger.annotations.Api;

@Api(value = "DataTemplate", description = "REST resource for data template")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/datatemplate")
public class DataTemplateResource {

    @Inject
    private DataTemplateService dataTemplateService;

    @PostMapping("")
    public String create(@PathVariable String customerSpace, @RequestBody DataTemplate dataTemplate) {
        return dataTemplateService.create(dataTemplate);
    }

}
