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
                "Accounting",
                "Administrative",
                "Arts and Design",
                "Business Development",
                "Community & Social Services",
                "Consulting",
                "Education",
                "Engineering",
                "Entrepreneurship",
                "Finance",
                "Healthcare Services",
                "Human Resources",
                "Information Technology",
                "Legal",
                "Marketing",
                "Media & Communications",
                "Military & Protective Services",
                "Operations",
                "Product Management",
                "Program & Product Management",
                "Purchasing",
                "Quality Assurance",
                "Real Estate",
                "Research",
                "Sales",
                "Support",
                "Other"
        );
    }

    // replaced by get job levels
    @Deprecated
    @GetMapping("/tps-titles")
    public List<String> getTpsTitles() {
        return Arrays.asList( //
                "Partner", //
                "Owner", //
                "CXO", //
                "VP", //
                "Director", //
                "Manager", //
                "Senior", //
                "Entry", //
                "Other" //
        );
    }

    @GetMapping("/tps-job-levels")
    public List<String> getTpsJobLevels() {
        return Arrays.asList( //
                "Partner", //
                "Owner", //
                "CXO", //
                "VP", //
                "Director", //
                "Manager", //
                "Senior", //
                "Entry", //
                "Other" //
        );
    }

}
