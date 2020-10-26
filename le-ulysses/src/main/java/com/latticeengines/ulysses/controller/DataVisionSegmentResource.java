package com.latticeengines.ulysses.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.ListSegment;

import io.swagger.annotations.Api;

@Api(value = "APIs datavision segment")
@RestController
@RequestMapping("/datavision/segments")
public class DataVisionSegmentResource {

    @GetMapping
    public String getSegments() {
        return "Hello world!";
    }

    @PostMapping
    public ListSegment createSegment() {
        return null;
    }

    @PutMapping
    public ListSegment updateSegment() {
        return null;
    }

}
