
package org.apache.doris.httpv2.rest;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.plandetail.PlanDetailMgr;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
public class PlanDetailAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(PlanDetailAction.class);

    @RequestMapping(path = "/api/plan_detail", method = RequestMethod.GET)
    protected Object planDetail(HttpServletRequest request, HttpServletResponse response) {
        try {
            String queryIdStr = request.getParameter("query_id");
            if (queryIdStr == null || queryIdStr.isEmpty()) {
                return ResponseEntityBuilder.badRequest("Missing query_id");
            }

            String planDetailFile = PlanDetailMgr.getInstance().getPlanDetailFilePath(queryIdStr);
            LOG.info("plan detail file name: {}", planDetailFile);
            File file = new File(planDetailFile);
            if (!file.exists()) {
                return ResponseEntityBuilder.notFound("File not found: " + planDetailFile);
            }

            getFile(request, response, file, file.getName());

            return null;
        } catch (IOException e) {
            return ResponseEntityBuilder.internalError("Failed to read file: " + e.getMessage());
        } catch (Exception e) {
            return ResponseEntityBuilder.internalError("Internal server error: " + e.getMessage());
        }
    }

    public static String generatePlanDetailURL(String queryID) {
        Env envInstance = Env.getCurrentEnv();
        String selfHost = envInstance.getSelfNode().getHost();
        int httpPort = Config.http_port;
        return String.format(
                "http://%s:%d/api/plan_detail?query_id=%s",
                selfHost, httpPort, queryID);
    }
}