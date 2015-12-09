package com.fusionskye.ezsonar.message.model;

public class Connection {

    /**
     * 源node
     */
    private String source;
    /**
     * 目标node
     */
    private String target;

    /**
     * 流ID
     */
    private String streamid;

    public Connection(String source, String target, String streamid) {
        this.source = source;
        this.target = target;
        this.streamid = streamid;
    }

    public String getSource() {
        return source;
    }

    public String getStreamid() {
        return streamid;
    }

    public String getTarget() {
        return target;
    }
}
