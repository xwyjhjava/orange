package com.dreams.common;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.FormatStyle;
import java.util.Date;
import java.util.Locale;

/**
 * @author ming
 * @version V1.0
 * @Package com.dreams.common
 * @date 2019/12/30 10:09
 * @descrition 约定：日期是年月日，时间是时分秒
 */
public class LocalDateUtils {

    public static void main(String[] args) {

        System.out.println(getDateTimeDiff("2019-12-20 23:59:50", "2019-12-30 23:15:26", "yyyy-MM-dd HH:mm:ss", "day"));

        System.out.println(getDateBefore("2019-12-30", 10, false, "yyyy-MM-dd"));

        System.out.println(getPeriodOfDay("03:53:20", "HH:mm:ss"));

        System.out.println(getLocalByGMT(getGMTInstant().toString()));

        System.out.println(getDatetimeFromInstant(LocalDateTime.now().toEpochSecond(ZoneOffset.of("+8"))));
    }

    //get Date

    /**
     *
     * @param date 字符串日期, 传auto时默认是当前日期
     * @param format 日期格式
     * @return LocalDate
     */
    public static LocalDate getDate(String date, String format){
        if("auto".equals(date)) {
            return LocalDate.now();
        }else{
            DateTimeFormatter formatter =  DateTimeFormatter.ofPattern(format);
            return LocalDate.parse(date, formatter);
        }
    }

    /**
     *
     * @param time 字符串时间，传auto默认是当前时间
     * @param format 时间格式化
     * @return LocalTime
     */
    public static LocalTime getTime(String time, String format){
        if("auto".equals(time)) {
            return LocalTime.now();
        }else{
            DateTimeFormatter formatter =  DateTimeFormatter.ofPattern(format);
            return LocalTime.parse(time, formatter);
        }
    }

    //获得GMT时间
    public static Instant getGMTInstant(){
        return Instant.now();
    }

    //两个时间相差秒数

    /**
     *
     * @param startDate 开始时间
     * @param endDate   结束时间
     * @param format    时间格式化
     * @param choose    选择返回秒数还是天数
     * @return  相差秒数或天数，返回的不是绝对值
     */
    public static long getDateTimeDiff(String startDate, String endDate, String format, String choose){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
        LocalDateTime start = LocalDateTime.parse(startDate, formatter);
        LocalDateTime end = LocalDateTime.parse(endDate, formatter);
        if ("second".equals(choose)) {
            return Duration.between(start, end).getSeconds();
        }else if("day".equals(choose)){
            return Period.between(start.toLocalDate(), end.toLocalDate()).getDays();
        }else{
            return 0;
        }
    }

    //出生日期计算年龄

    /**
     *
     * @param birth 出生日期
     * @param format 日期格式化
     * @return 年龄
     */
    public static int getAgeFromBirth(String birth, String format){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
        LocalDate start = LocalDate.parse(birth, formatter);
        LocalDate end = LocalDate.now();
        return Period.between(start, end).getYears();
    }



    /**
     *
     * @param dateTime 传入的时间
     * @param format 时间格式化
     * @return 时间戳（秒）
     * @description localDateTime转时间戳(秒)
     */
    public static long getInstantFromDatetime(String dateTime, String format){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
        return LocalDateTime.parse(dateTime, formatter).toEpochSecond(ZoneOffset.of("+8"));
    }

    /**
     *
     * @param timestap 时间戳
     * @return localDateTime
     * @description 时间戳(秒) 转localDateTime
     */
    public static LocalDateTime getDatetimeFromInstant(long timestap){
        Instant instant = Instant.ofEpochSecond(timestap);
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneOffset.systemDefault());
        return dateTime;
    }

    //获取某日期前或后某一天的日期

    /**
     *
     * @param date 传入的日期
     * @param days  天数
     * @param isBefore  false为日期后，true为日期后
     * @param format    日期格式化
     * @return  间隔天数的日期
     */
    public static LocalDate getDateBefore(String date, long days, boolean isBefore, String format){

        DateTimeFormatter formatter =  DateTimeFormatter.ofPattern(format);
        LocalDate localDate = LocalDate.parse(date, formatter);
        LocalDate result = LocalDate.now();
        if(isBefore){
            result = localDate.minusDays(days);
        }else {
            result = localDate.plusDays(days);
        }
        return result;
    }

    //判断是AM还是PM

    /**
     *
     * @param time 传入的时间
     * @param format 时间格式化
     * @return 返回该时间是AM还是PM
     */
    public static String getPeriodOfDay(String time, String format){

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
        LocalTime flagTime = LocalTime.of(12,0,0);
        LocalTime localTime = LocalTime.parse(time, formatter);
        boolean flag = localTime.isAfter(flagTime);
        if(flag){
            return "PM";
        }else{
            return "AM";
        }
    }

    //GMT转本地时间

    /**
     *  DateTimeFormatter formatter = DateTimeFormatter
     *                 .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
     * @param instant 格林威治标准时间
     * @return
     */
    public static LocalDateTime getLocalByGMT(String instant){

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        Instant localInstant = Instant.parse(instant);
        //北京是东八区
        String localDateTimeStr = LocalDateTime
                .ofInstant(localInstant, ZoneOffset.of("+8"))
                .format(formatter);
        return LocalDateTime.parse(localDateTimeStr, formatter);
    }

}
