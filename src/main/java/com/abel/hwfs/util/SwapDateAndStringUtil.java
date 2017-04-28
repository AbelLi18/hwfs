package com.abel.hwfs.util;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

public class SwapDateAndStringUtil {

    private static Logger log = Logger.getLogger(SwapDateAndStringUtil.class);

    /**
     * 日期转换成字符串
     *
     * @param date
     * @return str
     */
    public static String DateToStr(Date date) {

        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        String str = format.format(date);
        return str;
    }

    /**
     * 字符串转换成日期
     *
     * @param str
     * @return date
     * @exception String
     *                format fail, the method will return a new Date(Thu Jan 01
     *                08:00:00 CST 1970).
     */
    public static Date StrToDate(String str) {

        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        Date date = null;
        try {
            date = format.parse(str);
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
            date = new Date(0);
        }
        return date;
    }
}
