package com.latticeengines.propdata.collection.testframework;

import java.io.IOException;
import java.util.Collections;
import java.util.Random;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.mortbay.jetty.HttpStatus;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.propdata.workflow.collection.PublicationWorkflowConfiguration;

public class PublicationWorkflowServlet extends HttpServlet {

    private static final long serialVersionUID = -1137567298959679486L;
    private Log log = LogFactory.getLog(PublicationWorkflowServlet.class);
    private Random random = new Random(System.currentTimeMillis());
    private PayloadVerifier verifier;

    public PublicationWorkflowServlet(PayloadVerifier verifier) {
        this.verifier = verifier;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        resp.setContentType("application/json");

        String payload = IOUtils.toString(req.getInputStream());
        PublicationWorkflowConfiguration configuration = JsonUtils.deserialize(payload, PublicationWorkflowConfiguration.class);
        verifier.verify(configuration);

        String appIdStr = String.format("application_%d_%04d", System.currentTimeMillis(), random.nextInt(10000));
        ApplicationId appId = ConverterUtils.toApplicationId(appIdStr);
        AppSubmission appSubmission = new AppSubmission(Collections.singletonList(appId));
        resp.getWriter().write(JsonUtils.serialize(appSubmission));
        resp.setStatus(HttpStatus.ORDINAL_200_OK);
    }

    public interface PayloadVerifier {
        void verify(PublicationWorkflowConfiguration configuration);
    }

}
