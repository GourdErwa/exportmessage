package com.fusionskye.ezsonar.message.model;

import org.bson.types.BasicBSONList;

/**
 * @author wei.Li by 16/3/16
 */
public class Stream {

    private String id;

    private BasicBSONList success_ret_codes;

    public Stream(String id, BasicBSONList success_ret_codes) {
        this.id = id;
        this.success_ret_codes = success_ret_codes;
    }

    public String getId() {
        return id;
    }

    public BasicBSONList getSuccess_ret_codes() {
        return success_ret_codes;
    }
}
