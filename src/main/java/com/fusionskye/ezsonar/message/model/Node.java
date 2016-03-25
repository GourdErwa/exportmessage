package com.fusionskye.ezsonar.message.model;

import com.fusionskye.ezsonar.message.service.TopoService;
import com.fusionskye.ezsonar.message.util.QueryCondition;
import com.google.common.base.MoreObjects;
import org.elasticsearch.common.Nullable;

import java.util.Arrays;

/**
 * 节点类
 */
public class Node {

    static final String IN = "in";//流入
    static final String OUT = "out";//流出
    static final String DOUBLE = "double";//两边

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

    /**
     * 统计此节点数据所需流id 对应组合的成功数量过滤条件 是否已设置
     * 2016年03月25日
     * 如果为 null 表示未设置成功返回码,成功标识符位显示 n/a
     */
    private
    @Nullable
    boolean isSettingSuccessRetCodes;

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

    public String[] getStatisticsStreamIds() {
        return statisticsStreamIds == null ? null : Arrays.copyOf(statisticsStreamIds, statisticsStreamIds.length);
    }

    public void setStatisticsStreamIds(String[] statisticsStreamIds) {
        if (statisticsStreamIds == null) {
            this.statisticsStreamIds = null;
        } else {
            final int length = statisticsStreamIds.length;
            this.statisticsStreamIds = Arrays.copyOf(statisticsStreamIds, length);
            final Stream[] streams = new Stream[length];
            for (int i = 0; i < length; i++) {
                streams[i] = TopoService.getStream(statisticsStreamIds[i]);
            }
            this.isSettingSuccessRetCodes = QueryCondition.isSettingSuccessRetCodes(streams);
        }
    }

    public boolean getSettingSuccessRetCodes() {
        return isSettingSuccessRetCodes;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("node_id", node_id)
                .add("name", name)
                .add("streamDirection", streamDirection)
                .add("statisticsStreamIds", statisticsStreamIds == null ? null : Arrays.toString(statisticsStreamIds))
                .add("isSettingSuccessRetCodes", isSettingSuccessRetCodes)
                .toString();
    }

}
