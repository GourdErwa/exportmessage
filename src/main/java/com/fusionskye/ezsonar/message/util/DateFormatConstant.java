package com.fusionskye.ezsonar.message.util;


import org.apache.logging.log4j.core.util.datetime.FastDateFormat;

import java.util.Locale;

/**
 * 时间格式化相关常量
 * <p/>
 * SimpleDateFormat 是线程不安全的
 *
 * @author wei.Li by 15/8/28
 */
public interface DateFormatConstant {

    /**
     * yyyy-MM-dd
     */
    FastDateFormat FAST_DATE_FORMAT_1 = FastDateFormat.getInstance("yyyy-MM-dd", Locale.CHINA);

    /**
     * yyyyMMdd
     */
    FastDateFormat FAST_DATE_FORMAT_2 = FastDateFormat.getInstance("yyyy/MM/dd HH:mm", Locale.CHINA);


    /**
     * yyyyMMddHHmmss
     */
    FastDateFormat FAST_DATE_FORMAT_3 = FastDateFormat.getInstance("yyyyMMddHHmmss", Locale.CHINA);


}
