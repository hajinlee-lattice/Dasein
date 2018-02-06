package com.latticeengines.playmaker.controller;

import java.io.IOException;

import javax.servlet.http.HttpServletResponse;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import springfox.documentation.annotations.ApiIgnore;

@ApiIgnore
@RestController
@RequestMapping
public class SwaggerResource {

    @GetMapping("/playmaker/v2/api-docs")
    public void redirectApiDocs(HttpServletResponse response) throws IOException {
        response.sendRedirect("/v2/api-docs");
    }

}
