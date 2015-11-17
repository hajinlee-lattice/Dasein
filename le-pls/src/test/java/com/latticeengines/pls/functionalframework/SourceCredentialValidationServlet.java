package com.latticeengines.pls.functionalframework;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.HttpStatus;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;

@WebServlet("/eai/validatecredential/customerspaces/*")
public class SourceCredentialValidationServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("application/json");
        resp.getWriter().write(SimpleBooleanResponse.successResponse().toString());
        resp.setStatus(HttpStatus.OK_200);
    }

}
