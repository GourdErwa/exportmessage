package com.fusionskye.ezsonar.message.model;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * 业务路径的MongoDB映射定义
 */
public class Topo {

    private static final Logger LOGGER = LoggerFactory.getLogger(Topo.class);

    private static final Node EMPTY_NODE_OBJ = new Node();
    private static final String[] EMPTY_STRING_ARRAY = {};

    private String id;

    private String name;

    private List<Node> nodeList;

    private List<Connection> connList;

    public Topo(String id, String name, List<Node> nodeList, List<Connection> connList) {
        this.id = id;
        this.name = name;
        this.nodeList = nodeList;
        this.connList = connList;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public List<Node> getNodeList() {
        return nodeList;
    }

    public List<Connection> getConnList() {
        return connList;
    }

    /**
     * 根据 nodeId 获取对应的配置指标计算流{@link Node#IN Node#OUT Node#DOUBLE}
     *
     * @param nodeNames nodeNames
     * @return 未匹配到的 nodeName
     */
    public List<String> fetchNodeNameStreamIdsByStreamDirection(List<String> nodeNames) {

        checkNotNull(nodeNames);
        checkNotNull(this.nodeList);

        Iterator<String> iterator = nodeNames.iterator();
        while (iterator.hasNext()) {
            String next = iterator.next();
            for (Node node : this.nodeList) {
                final String nodeName = node.getName();
                if (next.equals(nodeName)) {
                    final String[] streamIds = fetchNodeIdStreamIdsByStreamDirection(node.getNode_id());
                    if (streamIds != null) {
                        node.setStatisticsStreamIds(streamIds);
                        iterator.remove();
                    } else {
                        LOGGER.error("配置文件解析为 待统计路径 id={} name={} , 节点名称={} 未找到统计该节点数据所需的流,默认不进行统计",
                                this.id, this.name, nodeName);
                    }
                    break;
                }
            }
        }

        final Iterator<Node> nodeIterator = this.nodeList.iterator();
        while (nodeIterator.hasNext()) {
            if (nodeIterator.next().getStatisticsStreamIds() == null) {
                nodeIterator.remove();
            }
        }

        return nodeNames;
    }

    /**
     * 根据 nodeId 获取对应的配置指标计算流{@link Node#IN Node#OUT Node#DOUBLE}
     *
     * @param nodeId nodeId
     * @return 流Ids or null
     */
    private String[] fetchNodeIdStreamIdsByStreamDirection(String nodeId) {

        String[] streamInfo = null;

        final Node node = fetchNode(nodeId);
        if (node == null) {
            return EMPTY_STRING_ARRAY;
        }

        // 判断客户端属性，获得出口流
        final String streamDirection = node.getStreamDirection();
        if (streamDirection == null) {
            return EMPTY_STRING_ARRAY;
        }

        if (streamDirection.equals(Node.OUT)) {
            streamInfo = fetchNodeStreamIdsByFlowType(nodeId, Node.OUT);
        } else if (streamDirection.equals(Node.IN)) {
            streamInfo = fetchNodeStreamIdsByFlowType(nodeId, Node.IN);
        } else if (streamDirection.equals(Node.DOUBLE)) {
            streamInfo = fetchNodeStreamIdsByFlowType(nodeId, Node.IN);
        }
        return streamInfo;
    }

    /**
     * 获取node_id 对应的Node
     *
     * @param nodeId nodeId
     * @return node 对象 OR null
     */
    private Node fetchNode(String nodeId) {
        final List<Node> nodeList = getNodeList();
        if (nodeList != null && nodeId != null) {
            for (Node node : nodeList) {
                if (node.getNode_id().equalsIgnoreCase(nodeId)) {
                    return node;
                }
            }
        }
        return EMPTY_NODE_OBJ;
    }

    /**
     * 根据nodeId和进出类型， 查找对应的流id列表
     * 同一个节点上，如果有多个流，则叠加
     *
     * @param nodeId   nodeId
     * @param flowType flowType
     * @return 流id列表 or null
     */
    private String[] fetchNodeStreamIdsByFlowType(String nodeId, String flowType) {

        List<String> retList = Lists.newArrayList();
        final List<Connection> connList = this.getConnList();
        if (!(connList == null || nodeId == null)) {
            for (Connection conn : connList) {
                String targetNodeId = flowType.equalsIgnoreCase(Node.IN) ? conn.getTarget() : conn.getSource();

                if (targetNodeId != null && targetNodeId.equalsIgnoreCase(nodeId)) {
                    if (!(conn.getStreamid() == null || conn.getStreamid().equals(""))) {
                        retList.add(conn.getStreamid());
                    }
                }
            }
        }
        return retList.isEmpty() ? EMPTY_STRING_ARRAY : retList.toArray(new String[retList.size()]);
    }

}
