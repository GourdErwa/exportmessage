package com.fusionskye.ezsonar.message.model;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author wei.Li by 15/12/8
 */
public class SearchVo {

    private static final DateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm");
    private static final DateFormat SIMPLE_FILE_NAME_DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");
    //(ms)
    private long startTime;
    private String startTimeFormat;
    private String fileNameStartTimeFormat;

    //(ms)
    private long endTime;

    private String topoId;
    private String topoName;

    private Node node;
    //查询结果数据
    private List<Map<String, Object>> data = Lists.newArrayList();


    public SearchVo(long startTime, long endTime, String topoId, String topoName, Node node) {
        this.startTime = startTime;
        final Date date = new Date(startTime);
        this.startTimeFormat = SIMPLE_DATE_FORMAT.format(date);
        this.fileNameStartTimeFormat = SIMPLE_FILE_NAME_DATE_FORMAT.format(date);
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
            this.data.add(transformDataHandle(Maps.<String, Object>newHashMap(), linkedNode));
        }
    }

    private Map<String, Object> transformDataHandle(Map<String, Object> map, LinkedNode linkedNode) {
        map.put(linkedNode.getKey(), linkedNode.getValue());
        final LinkedNode next = linkedNode.getNext();
        if (next != null) {
            transformDataHandle(map, next);
        }
        return map;
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
