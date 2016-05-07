package com.latticeengines.pls.functionalframework;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.latticeengines.domain.exposed.metadata.Category;
import org.eclipse.jetty.http.HttpStatus;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.StatisticalType;
import com.latticeengines.domain.exposed.metadata.Tag;

public class PropDataLeadEnrichmentAttributeServlet extends HttpServlet {

    private static final long serialVersionUID = -6068276954643550403L;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("application/json");

        ColumnMetadata[] columns = new ColumnMetadata[2];
        ColumnMetadata column = new ColumnMetadata();
        column.setColumnName("TechIndicator_AddThis");
        column.setDisplayName("Add This");
        column.setDataType("NVARCHAR(50)");
        column.setMatchDestination("BuiltWithPivoted");
        column.setTagList(Collections.singletonList(Tag.EXTERNAL));
        column.setFundamentalType(FundamentalType.BOOLEAN);
        column.setStatisticalType(StatisticalType.ORDINAL);
        column.setDescription("Tech Indicator Add This");
        column.setApprovedUsageList(Arrays.asList(ApprovedUsage.MODEL));
        column.setCategory(Category.WEBSITE_PROFILE);
        columns[0] = column;
        column = new ColumnMetadata();
        column.setColumnName("TechIndicator_RemoveThis");
        column.setDisplayName("Remove This");
        column.setDataType("NVARCHAR(100)");
        column.setMatchDestination("HGData");
        column.setTagList(Collections.singletonList(Tag.EXTERNAL));
        column.setFundamentalType(FundamentalType.ALPHA);
        column.setStatisticalType(StatisticalType.INTERVAL);
        column.setDescription("Tech Indicator Remove This");
        column.setApprovedUsageList(Collections.singletonList(ApprovedUsage.MODEL_MODELINSIGHTS));
        column.setCategory(Category.WEBSITE_PROFILE);
        columns[1] = column;
        String json = JsonUtils.serialize(columns);
        resp.getWriter().write(json);

        resp.setStatus(HttpStatus.OK_200);
    }
}
