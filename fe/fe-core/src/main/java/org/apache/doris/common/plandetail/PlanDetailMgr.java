// added

package org.apache.doris.common.plandetail;

import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.thrift.TUniqueId;

import org.apache.thrift.transport.TTransportException;

public class PlanDetailMgr {
    private static final PlanDetailMgr INSTANCE = new PlanDetailMgr();

    private PlanDetailMgr() {}

    public static PlanDetailMgr getInstance() {
        return INSTANCE;
    }

    public String getPlanDetailFilePath(String queryID) {
        String file = Config.tmp_dir + "/plandetail-" + queryID + ".txt";
        return file;
    }

    public PlanDetailWriteStorage create(TUniqueId queryId) throws TTransportException {
        return new PlanDetailWriteStorage(getPlanDetailFilePath(DebugUtil.printId(queryId)));
    }
}