package edu.ldzm.utils;

import java.text.SimpleDateFormat;
import java.util.Date;


public class DateUtil {
	
	/**
	 * 标准日期格式yyyyMMddHHmmss
	 */
    public static final SimpleDateFormat STD_INPUT_DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");
    /**
     * 使用标准参数日期格式yyyyMMddHHmmss来格式化日期
     */
    public static String date2String(Date date) {
    	return STD_INPUT_DATE_FORMAT.format(date);
    }
}
