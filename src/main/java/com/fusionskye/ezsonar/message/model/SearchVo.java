package com.fusionskye.ezsonar.message.model;

import com.fusionskye.ezsonar.message.util.DateFormatConstant;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.*;

/**
 * 导出每个 cvs 文件需要的数据源封装
 *
 * @author wei.Li by 15/12/8
 */
public class SearchVo {

    //(ms)
    private long startTime;
    //(ms)
    private long endTime;
    //统计时间点
    private String startTimeFormat;
    //文件名称 默认为统计时间点
    private String fileNameStartTimeFormat;

    private String topoId;
    private String topoName;
    //节点信息
    private Node node;
    //查询结果数据
    private List<Map<String, Object>> data = Lists.newArrayList();

    public SearchVo(long startTime, long endTime, String topoId, String topoName, Node node) {
        this.startTime = startTime;
        final Date date = new Date(startTime);
        this.startTimeFormat = DateFormatConstant.FAST_DATE_FORMAT_2.format(date);
        this.fileNameStartTimeFormat = DateFormatConstant.FAST_DATE_FORMAT_3.format(date);
        this.endTime = endTime;
        this.topoId = topoId;
        this.topoName = topoName;
        this.node = node;
    }

    public long getStartTime() {
        return startTime;
    }

    public String getStartTimeFormat() {
        return startTimeFormat;
    }

    public String getFileNameStartTimeFormat() {
        return fileNameStartTimeFormat;
    }

    public long getEndTime() {
        return endTime;
    }

    public String getTopoId() {
        return topoId;
    }

    public String getTopoName() {
        return topoName;
    }

    public Node getNode() {
        return node;
    }

    public List<Map<String, Object>> getData() {
        return data;
    }

    /**
     * 将 LinkedNode 内容转换为 Map<String, Object>
     *
     * @param data data
     */
    public void setData(List<LinkedNode> data) {
        if (data != null) {
            transformData(data);
        }
    }

    private void transformData(List<LinkedNode> linkedNodes) {
        for (LinkedNode linkedNode : linkedNodes) {
            final List<Map<String, Object>> maps = eachLinkNodeDatas(linkedNode);
            this.data.addAll(maps);
        }
    }

    /**
     * 遍历某个节点数据
     *
     * @param linkedNode 待遍历节点
     * @return 节点数据
     */
    private List<Map<String, Object>> eachLinkNodeDatas(LinkedNode linkedNode) {

        final ArrayList<Map<String, Object>> maps = Lists.newArrayList();
        final HashMap<String, Object> upper = Maps.newHashMap();
        upper.put(linkedNode.getKey(), linkedNode.getValue());

        eachLinkNodeDatasHandle(maps, upper, linkedNode);
        return maps;
    }

    /**
     * 遍历某个节点数据
     *
     * @param maps       节点所有数据
     * @param upper      当前节点数据
     * @param linkedNode 遍历节点
     */
    private void eachLinkNodeDatasHandle(List<Map<String, Object>> maps, Map<String, Object> upper, LinkedNode linkedNode) {

        final List<LinkedNode> next = linkedNode.getLinkedNodes();

        if (next == null || next.isEmpty()) {
            return;
        }

        for (LinkedNode node : next) {

            final String nodeKey = node.getKey();
            final Object nodeValue = node.getValue();

            if (node.isGroup()) {

                Map<String, Object> map = Maps.newHashMap(upper);
                map.put(nodeKey, nodeValue);
                maps.add(map);
                eachLinkNodeDatasHandle(maps, map, node);
            } else {
                upper.put(nodeKey, nodeValue);
                eachLinkNodeDatasHandle(maps, upper, node);
            }

        }
    }


    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("startTime", startTime)
                .add("startTimeFormat", startTimeFormat)
                .add("endTime", endTime)
                .add("topoId", topoId)
                .add("topoName", topoName)
                .add("node", node)
                .add("data", data)
                .toString();
    }
}
