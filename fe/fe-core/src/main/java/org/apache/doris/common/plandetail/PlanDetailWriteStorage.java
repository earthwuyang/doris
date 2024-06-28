
package org.apache.doris.common.plandetail;

import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineInstanceParams;
import org.apache.doris.thrift.TUniqueId;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.apache.thrift.transport.TTransportException;

import java.io.File;
import java.util.List;
import java.util.Map;

public class PlanDetailWriteStorage {
    private String planDetailFilePath;
    private ObjectMapper objectMapper;
    private TSerializer serializer;

    private Map<String, Object> executionExplain;
    private Map<String, Object> fragmentsExplain;
    private Map<String, Object> fragmentsDetailExplain;
    private List<Object> fragmentInstancesExplain;

    public PlanDetailWriteStorage(String planDetailFilePath) throws TTransportException {
        this.planDetailFilePath = planDetailFilePath;
        this.objectMapper = new ObjectMapper();
        this.serializer = new TSerializer(new TSimpleJSONProtocol.Factory());
    }

    private void initPlanDetail(String engineType) {
        executionExplain = Maps.newLinkedHashMap();
        executionExplain.put("Engine Type", engineType);
        fragmentsExplain = Maps.newLinkedHashMap();
        executionExplain.put(engineType + " Execution Plan", fragmentsExplain);
        fragmentsDetailExplain = Maps.newLinkedHashMap();
        fragmentsExplain.put("Fragments", fragmentsDetailExplain);
    }

    public void initPipelinePlanDetail() {
        initPlanDetail("Pipeline");
    }

    public void initNormalPlanDetail() {
        initPlanDetail("Normal");
    }

    public void addFragment(int fragment) {
        fragmentInstancesExplain = Lists.newArrayList();
        fragmentsDetailExplain.put(String.format("Fragment %d", fragment), fragmentInstancesExplain);
    }

    public void addTPipelineFragmentParams(TNetworkAddress address, TPipelineFragmentParams params)
            throws UserException {
        try {
            for (TPipelineInstanceParams instanceParam : params.local_params) {
                instanceParam.setFragmentInstanceIdHumanReadable(
                        DebugUtil.printId(instanceParam.fragment_instance_id));
            }

            String json = serializer.toString(params);
            JsonNode jEntryValue = objectMapper.readTree(json);
            json = serializer.toString(address);
            JsonNode jEntryKey = objectMapper.readTree(json);

            if (jEntryValue.getNodeType() != JsonNodeType.OBJECT) {
                throw new UserException(
                        String.format("NodeType should be OBJECT, but now is %s", jEntryValue.getNodeType().name()));
            }

            ((ObjectNode) jEntryValue).set("host", jEntryKey);
            fragmentInstancesExplain.add(jEntryValue);
        } catch (Exception e) {
            throw new UserException("Error in addTPipelineFragmentParams", e);
        }
    }

    public void addTExecPlanFragmentParams(TNetworkAddress address, TExecPlanFragmentParams params,
            PlanFragmentId fragmentId, TUniqueId instanceId) throws UserException {
        try {
            String json = serializer.toString(params);
            JsonNode jTParam = objectMapper.readTree(json);

            if (jTParam.getNodeType() != JsonNodeType.OBJECT) {
                throw new UserException(
                        String.format("NodeType should be OBJECT, but now is %s", jTParam.getNodeType().name()));
            }

            ((ObjectNode) jTParam).put("fragment_id", fragmentId.asInt());
            ((ObjectNode) jTParam).put("fragment_instance_id_human_readable", DebugUtil.printId(instanceId));

            json = serializer.toString(address);
            JsonNode jEntryHost = objectMapper.readTree(json);
            ((ObjectNode) jTParam).set("host", jEntryHost);

            fragmentInstancesExplain.add(jTParam);
        } catch (Exception e) {
            throw new UserException("Error in addTExecPlanFragmentParams", e);
        }
    }

    public void finish() throws UserException {
        File file = new File(planDetailFilePath);
        try {
            if (!file.exists()) {
                file.createNewFile();
            }

            objectMapper.writeValue(file, executionExplain);
        } catch (Exception e) {
            throw new UserException("Error in finish", e);
        }
    }
}