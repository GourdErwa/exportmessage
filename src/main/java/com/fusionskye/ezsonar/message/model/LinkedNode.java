package com.fusionskye.ezsonar.message.model;

import com.google.common.base.MoreObjects;

import java.util.List;

/**
 * @author wei.Li by 15/12/8
 */
public class LinkedNode {

    private String key;
    private Object value;

    //数据是否进行分组
    private boolean isGroup = true;

    private List<LinkedNode> linkedNodes;

    public LinkedNode(String key, Object value) {
        this.key = key;
        this.value = value;
    }

    public LinkedNode(String key, Object value, boolean isGroup) {
        this.key = key;
        this.value = value;
        this.isGroup = isGroup;
    }

    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public boolean isGroup() {
        return isGroup;
    }

    public void setGroup(boolean group) {
        isGroup = group;
    }

    public List<LinkedNode> getLinkedNodes() {
        return linkedNodes;
    }

    public void setLinkedNodes(List<LinkedNode> linkedNodes) {
        this.linkedNodes = linkedNodes;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("key", key)
                .add("value", value)
                .add("next", linkedNodes)
                .toString();
    }
}
