package com.latticeengines.matchapi.controller;

import java.util.Arrays;
import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.Api;

@Api(value = "Contact Metadata")
@RestController
@RequestMapping("/contact-metadata")
public class ContactMetadataResource {

    @GetMapping("/tps-job-functions")
    public List<String> getTpsJobFunctions() {
        return Arrays.asList( //
                "manager", //
                "director", //
                "owner", //
                "technician", //
                "engineer" //
        );
    }

    @GetMapping("/tps-titles")
    public List<String> getTpsTitles() {
        return Arrays.asList( //
                "CEO", //
                "Consultant", //
                "Secretary", //
                "Accounting", //
                "Engineering" //
        );
    }

}
