package com.fusionskye.ezsonar.message.model;

import com.google.common.base.MoreObjects;

/**
 * 节点类
 */
public class Node {

    public static String IN = "in";//流入
    public static String OUT = "out";//流出
    public static String DOUBLE = "double";//两边

    /**
     * 节点ID
     */
    private String node_id;
    /**
     * 姓名
     */
    private String name;

    /**
     * 流的方向
     */
    private String streamDirection;

    /**
     * 统计此节点数据所需流id
     */
    private String[] statisticsStreamIds;

    public Node() {
    }

    public Node(String node_id, String name, String streamDirection) {
        this.node_id = node_id;
        this.name = name;
        this.streamDirection = streamDirection;
    }

    public String getNode_id() {
        return node_id;
    }

    public String getName() {
        return name;
    }

    public String getStreamDirection() {
        return streamDirection;
    }

    public void setStatisticsStreamIds(String[] statisticsStreamIds) {
        this.statisticsStreamIds = statisticsStreamIds;
    }
    public String[] getStatisticsStreamIds() {
        return statisticsStreamIds;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("node_id", node_id)
                .add("name", name)
                .toString();
    }
}
