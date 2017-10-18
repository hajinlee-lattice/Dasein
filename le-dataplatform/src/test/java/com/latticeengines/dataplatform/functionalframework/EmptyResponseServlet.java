package com.latticeengines.dataplatform.functionalframework;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.HttpStatus;


public class EmptyResponseServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    public EmptyResponseServlet() {
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        resp.setContentType("application/json");
        resp.getWriter().write("");
        resp.setStatus(HttpStatus.SC_OK);
    }

}
