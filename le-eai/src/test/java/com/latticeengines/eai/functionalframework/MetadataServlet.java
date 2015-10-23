package com.latticeengines.eai.functionalframework;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.http.HttpStatus;
import com.latticeengines.domain.exposed.metadata.Table;

@WebServlet("/metadata/customerspaces/*")
public class MetadataServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    private final Map<String, Table> map = new HashMap<>();;

    public MetadataServlet(List<Table> tables) {
        for (Table table : tables) {
            map.put(table.getName(), table);
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("application/json");
        String pathInfo = req.getPathInfo();
        String[] pathParts = pathInfo.split("/");
        String tableName = pathParts[1];
        Table table = map.get(tableName);
        resp.getWriter().write(table.toString());
        resp.setStatus(HttpStatus.OK_200);
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp) {
        resp.setContentType("application/json");
        resp.setStatus(HttpStatus.OK_200);
    }
}
