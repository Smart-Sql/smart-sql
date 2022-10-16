package org.tools;

import com.google.common.base.Strings;
import org.gridgain.internal.h2.api.ErrorCode;
import org.gridgain.internal.h2.expression.function.DateTimeFunctions;
import org.gridgain.internal.h2.expression.function.Function;
import org.gridgain.internal.h2.message.DbException;
import org.gridgain.internal.h2.util.DateTimeUtils;
import org.gridgain.internal.h2.util.StringUtils;
import org.gridgain.internal.h2.value.*;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MyFunction {
    /**
     * abs 绝对值，如果是 abs 函数，就直接转换成 Math.abs(a);
     * 输入参数为 number 类型
     * {'method_name': 'abs', 'ls_ps': [{'type': 'number'}], 'return_type': 'number'}
     *
     * insert into sql_to_java (id, method_name, return_type) values (1, 'abs', 'number')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (1, 0, 'number')
     */
    public static <T extends Number> T abs(T a) {
        if (a instanceof Integer)
        {
            return (T) a.getClass().cast(Math.abs((Integer) a));
        }
        else if (a instanceof Double)
        {
            return (T) a.getClass().cast(Math.abs((Double) a));
        }
        else if (a instanceof Long)
        {
            return (T) a.getClass().cast(Math.abs((Long) a));
        }
        else if (a instanceof Float)
        {
            return (T) a.getClass().cast(Math.abs((Float) a));
        }

        return null;
    }

    /**
     * acos 反正弦
     * {'method_name': 'acos', 'ls_ps': [{'type': 'double'}], 'return_type': 'double'}
     *
     * insert into sql_to_java (id, method_name, return_type) values (5, 'acos', 'double')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (5, 0, 'double')
     */
    public static double acos(double vs) {
        return Math.acos(vs);
    }

    /**
     * asin
     * {'method_name': 'asin', 'ls_ps': [{'type': 'double'}], 'return_type': 'double'}
     *
     * insert into sql_to_java (id, method_name, return_type) values (6, 'asin', 'double')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (6, 0, 'double')
     */
    public static double asin(double vs) {
        return Math.asin(vs);
    }

    /**
     * atan
     * {'method_name': 'atan', 'ls_ps': [{'type': 'double'}], 'return_type': 'double'}
     *
     * insert into sql_to_java (id, method_name, return_type) values (7, 'atan', 'double')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (7, 0, 'double')
     */
    public static double atan(double vs) {
        return Math.atan(vs);
    }

    /**
     * ceiling
     * {'method_name': 'ceiling', 'ls_ps': [{'type': 'double'}], 'return_type': 'double'}
     *
     * insert into sql_to_java (id, method_name, return_type) values (8, 'ceiling', 'double')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (8, 0, 'double')
     */
    public static double ceiling(double vs) {
        return Math.ceil(vs);
    }

    /**
     * cos
     * {'method_name': 'cos', 'ls_ps': [{'type': 'double'}], 'return_type': 'double'}
     *
     * insert into sql_to_java (id, method_name, return_type) values (9, 'cos', 'double')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (9, 0, 'double')
     */
    public static double cos(double vs) {
        return Math.cos(vs);
    }

    /**
     * cosh
     * {'method_name': 'cosh', 'ls_ps': [{'type': 'double'}], 'return_type': 'double'}
     *
     * insert into sql_to_java (id, method_name, return_type) values (10, 'cosh', 'double')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (10, 0, 'double')
     */
    public static double cosh(double vs) {
        return Math.cosh(vs);
    }

    /**
     * cot
     * {'method_name': 'cot', 'ls_ps': [{'type': 'double'}], 'return_type': 'double'}
     *
     * insert into sql_to_java (id, method_name, return_type) values (11, 'cot', 'double')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (11, 0, 'double')
     */
    public static double cot(double vs) {
        double d = Math.tan(vs);
        if (d == 0.0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1);
        }
        return ValueDouble.get(1. / d).getDouble();
    }

    /**
     * degrees
     * {'method_name': 'degrees', 'ls_ps': [{'type': 'double'}], 'return_type': 'double'}
     *
     * insert into sql_to_java (id, method_name, return_type) values (12, 'degrees', 'double')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (12, 0, 'double')
     */
    public static double degrees(double vs) {
        return Math.toDegrees(vs);
    }

    /**
     * exp
     * {'method_name': 'exp', 'ls_ps': [{'type': 'double'}], 'return_type': 'double'}
     *
     * insert into sql_to_java (id, method_name, return_type) values (13, 'exp', 'double')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (13, 0, 'double')
     */
    public static double exp(double vs) {
        return Math.exp(vs);
    }

    /**
     * floor
     * {'method_name': 'floor', 'ls_ps': [{'type': 'double'}], 'return_type': 'double'}
     *
     * insert into sql_to_java (id, method_name, return_type) values (14, 'floor', 'double')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (14, 0, 'double')
     */
    public static double floor(double vs) {
        return Math.floor(vs);
    }

    /**
     * ln
     * {'method_name': 'ln', 'ls_ps': [{'type': 'double'}], 'return_type': 'double'}
     *
     * insert into sql_to_java (id, method_name, return_type) values (15, 'ln', 'double')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (15, 0, 'double')
     */
    public static double ln(double vs) {
        if (vs <= 0D) {
            throw DbException.getInvalidValueException("LN() argument", vs);
        }
        return Math.log(vs);
    }

    /**
     * log
     * {'method_name': 'log', 'ls_ps': [{'type': 'double'}, {'type': 'double'}], 'return_type': 'double'}
     *
     * insert into sql_to_java (id, method_name, return_type) values (16, 'log', 'double')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (16, 0, 'double')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (16, 1, 'double')
     */
    public static double log(double v0, double v1) {
        double result = 0D;
        if (v1 >= 0D) {
            if (v1 == Math.E) {
                result = Math.log(v0);
            } else if (v1 == 10D) {
                result = Math.log10(v0);
            } else {
                result = Math.log(v0) / Math.log(v1);
            }
        } else {
            result = Math.log(v0);
        }
        return result;
    }

    /**
     * log10
     * {'method_name': 'log10', 'ls_ps': [{'type': 'double'}], 'return_type': 'double'}
     *
     * insert into sql_to_java (id, method_name, return_type) values (17, 'log10', 'double')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (17, 0, 'double')
     */
    public static double log10(double v0) {
        return Math.log10(v0);
    }

    /**
     * PI
     * {'method_name': 'PI', 'ls_ps': [{'type': 'double'}], 'return_type': 'double'}
     *
     * insert into sql_to_java (id, method_name, return_type) values (18, 'pi', 'double')
     */
    public static double pi() {
        return Math.PI;
    }

    /**
     * radians
     *
     * insert into sql_to_java (id, method_name, return_type) values (19, 'radians', 'double')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (19, 0, 'long')
     */
    public static double radians(long v0) {
        Random random = new Random();
        random.setSeed(v0);
        return random.nextDouble();
    }

    /**
     * roundMagic
     *
     * insert into sql_to_java (id, method_name, return_type) values (20, 'roundMagic', 'double')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (20, 0, 'double')
     */
    public static double roundMagic(double v0) {
        return my_roundMagic(v0);
    }

    /**
     * sign
     *
     * insert into sql_to_java (id, method_name, return_type) values (21, 'sign', 'int')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (21, 0, 'double')
     */
    public static int sign(double v0) {
        if (v0 > 0D) {
            return 1;
        } else if (v0 < 0D) {
            return -1;
        }
        return 0;
    }

    /**
     * zero
     *
     * insert into sql_to_java (id, method_name, return_type) values (22, 'zero', 'int')
     */
    public static int zero() {
        return 0;
    }

    /**
     * ascii
     * */
    /*
    public static String ascii(String s)
    {
        if (s.isEmpty()) {
            return ValueNull.INSTANCE.getString();
        } else {
            return ValueInt.get(s.charAt(0)).getString();
        }
    }
    */

    /**
     * char
     *
     * insert into sql_to_java (id, method_name, return_type) values (23, 'my_char', 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (23, 0, 'double')
     */
    public static String my_char(int v0) {
        return ValueString.get(String.valueOf((char) v0)).getString();
    }

    /**
     * length
     *
     * insert into sql_to_java (id, method_name, return_type) values (23, 'length', 'long')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (23, 0, 'String')
     */
    public static long length(String v0) {
        if (!Strings.isNullOrEmpty(v0)) {
            return v0.length();
        }
        return 0L;
    }

    /**
     * concat
     *
     * insert into sql_to_java (id, method_name, return_type) values (24, 'concat', 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (24, 0, 'String...')
     */
    public static <T> String concat(T... vs) {
        StringBuilder stringBuilder = new StringBuilder();
        if (vs != null) {
            for (T v : vs) {
                stringBuilder.append(v);
            }
        }
        return stringBuilder.toString();
    }
    /*
    public static String concat(String... vs) {
        StringBuilder stringBuilder = new StringBuilder();
        if (vs != null) {
            for (String v : vs) {
                stringBuilder.append(v);
            }
        }
        return stringBuilder.toString();
    }
    */

    /**
     * 字符串转换为大写
     *
     * insert into sql_to_java (id, method_name, return_type) values (25, 'ucase', 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (25, 0, 'String')
     */
    public static String ucase(String v) {
        if (v != null) {
            return v.toUpperCase();
        }
        return "";
    }

    /**
     * 工作日名称
     */
    public static int day_name(Timestamp timestamp) {
        throw DbException.getUnsupportedException("暂时不支持 day_name 函数");
    }

    /**
     * 工作日名称
     */
    public static int year(Timestamp timestamp) {
        throw DbException.getUnsupportedException("暂时不支持 year 函数");
    }

    /**
     * @param ps 字符串转Double
     *
     * insert into sql_to_java (id, method_name, return_type) values (26, 'to_number', 'Double')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (26, 0, 'String')
     */
    public static Double to_number(final String ps) {
        return Double.parseDouble(ps);
    }

    /**
     * 整型转字符串
     *
     * insert into sql_to_java (id, method_name, return_type) values (27, 'to_char', 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (27, 0, 'Integer')
     */
    public static String to_char_int(Integer v) {
        return String.valueOf(v.intValue());
    }

    public static String to_char_long(Long v) {
        return String.valueOf(v.intValue());
    }

    /**
     * 浮点转字符串
     *
     * insert into sql_to_java (id, method_name, return_type) values (270, 'to_char', 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (270, 0, 'Double')
     */
    public static String to_char_double(Double v) {
        return String.valueOf(v.doubleValue());
    }

    /**
     * @param ps     字符串
     * @param format 格式
     *
     * insert into sql_to_java (id, method_name, return_type) values (28, 'to_char', 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (28, 0, 'Date')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (28, 1, 'String')
     */
    public static String to_char_date(final Date ps, final String format) {
        SimpleDateFormat sdf = null;
        switch (format) {
            case "yyyy-MM-dd":
                sdf = new SimpleDateFormat("yyyy-MM-dd");
                return sdf.format(ps);
            case "yyyy-MM":
                sdf = new SimpleDateFormat("yyyy-MM");
                return sdf.format(ps);
            case "yyyy-MM-dd HH:mm:ss":
                sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                return sdf.format(ps);
            case "yyyymmddhh24miss":
                sdf = new SimpleDateFormat("yyyyMMddHHmmss");
                return sdf.format(ps);
            case "yyyymmdd":
                sdf = new SimpleDateFormat("yyyyMMdd");
                return sdf.format(ps);
            case "yyyy/mm/dd":
                sdf = new SimpleDateFormat("yyyy/MM/dd");
                return sdf.format(ps);
            case "yyyy":
                sdf = new SimpleDateFormat("yyyy");
                return sdf.format(ps);
            case "mm":
                sdf = new SimpleDateFormat("MM");
                return sdf.format(ps);
            case "yyyymm":
                sdf = new SimpleDateFormat("yyyyMM");
                return sdf.format(ps);
            case "yyyymmdd hh24:mi:ss":
                sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
                return sdf.format(ps);
            case "ddhh24miss":
                sdf = new SimpleDateFormat("ddHHmmss");
                return sdf.format(ps);
        }
        return ps.toString();
    }

    /*
     * 月份加法处理
     *
     * insert into sql_to_java (id, method_name, return_type) values (29, 'add_months', 'Date')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (29, 0, 'Date')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (29, 1, 'Integer')
     * */
    public static Date add_months(final Date ps, final Integer num) {
        Calendar rightNow = Calendar.getInstance();
        rightNow.setTime(ps);
        rightNow.add(Calendar.MONTH, num);
        return rightNow.getTime();
    }

    /*
     * 字符串填充处理
     *
     * insert into sql_to_java (id, method_name, return_type) values (30, 'lpad', 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (30, 0, 'Object')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (30, 1, 'Object')
     * */
    public static <X, Y> String lpad(final X ps, final Y num)
    {
        return lpad(ps, num, null);
    }

    /**
     * 字符串填充处理
     *
     * insert into sql_to_java (id, method_name, return_type) values (31, 'lpad', 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (31, 0, 'Object')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (31, 1, 'Object')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (31, 2, 'Object')
     * */
    public static <X, Y, Z> String lpad(final X ps, final Y num, final Z s)
    {
        String ps_str = String.valueOf(ps);

        Integer num_v = StrToInteger(num.toString());

        String s_str = null;
        if (s != null && !Strings.isNullOrEmpty(s.toString()))
        {
            s_str = String.valueOf(s);
        }

        return ValueString.get(StringUtils.pad(ps_str,
                num_v, s_str == null ? null : s_str, false),
                true).getString();
    }

    private static Integer StrToInteger(final String ps)
    {
        String pattern = "^\\d+\\.\\d+$";
        Pattern p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE|Pattern.MULTILINE);
        Matcher m = p.matcher(ps);
        if (m.find())
        {
            return Integer.parseInt(ps.split("\\.")[0]);
        }
        else
        {
            pattern = "^\\d+$";
            p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE|Pattern.MULTILINE);
            m = p.matcher(ps);
            if (m.find())
            {
                return Integer.parseInt(ps);
            }
        }

        throw DbException.getInvalidValueException("请输入正确，能转换为数字的字符串！", ps);
    }

    /*
     * 字符串右填充处理
     *
     * insert into sql_to_java (id, method_name, return_type) values (32, 'rpad', 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (32, 0, 'Object')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (32, 1, 'Object')
     * */
    public static <X, Y> String rpad(final X ps, final Y num)
    {
        return rpad(ps, num, null);
    }

    /**
     * 字符串右填充处理
     *
     * insert into sql_to_java (id, method_name, return_type) values (33, 'rpad', 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (33, 0, 'Object')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (33, 1, 'Object')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (33, 2, 'Object')
     * */
    public static <X, Y, Z> String rpad(final X ps, final Y num, final Z s)
    {
        String ps_str = String.valueOf(ps);

        Integer num_v = StrToInteger(num.toString());

        String s_str = null;
        if (s != null && !Strings.isNullOrEmpty(s.toString()))
        {
            s_str = String.valueOf(s);
        }

        return ValueString.get(StringUtils.pad(ps_str,
                num_v, s_str == null ? null : s_str, true),
                true).getString();
    }

    /*
     * 字符串转date处理
     *
     * insert into sql_to_java (id, method_name, return_type) values (34, 'to_date', 'Date')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (34, 0, 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (34, 1, 'String')
     * */
    public static Date to_date(final String ps, final String format) {
        SimpleDateFormat sdf = null;
        switch (format) {
            case "yyyymm":
                sdf = new SimpleDateFormat("yyyyMM");
                try {
                    return sdf.parse(ps);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            case "yyyymmdd":
                sdf = new SimpleDateFormat("yyyyMMdd");
                try {
                    return sdf.parse(ps);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
        }
        return null;
    }

    /*
     *
     * 当月最后一天
     *
     * insert into sql_to_java (id, method_name, return_type) values (35, 'last_day', 'Date')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (35, 0, 'Date')
     * */
    public static Date last_day(final Date ps) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(ps);
        calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));

        //SimpleDateFormat lastDay= new SimpleDateFormat("yyyy-MM-dd");
        //return lastDay.format(calendar.getTime());
        return calendar.getTime();
    }

    /**
     * decode
     *
     * insert into sql_to_java (id, method_name, return_type) values (36, 'decode', 'Object')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (36, 0, 'Object...')
     */
    public static Object decode(List<Object> objects) {
        if (objects != null && objects.size() >= 0) {
            Object first = objects.get(0);
            for (int i = 1; i < objects.size() - 1; i = i + 2) {
                if (first.equals(objects.get(i))) {
                    return objects.get(i + 1);
                }
            }
        }
        return objects.get(objects.size() - 1);
    }

    /*
     * 正负判断
     *
     * insert into sql_to_java (id, method_name, return_type) values (37, 'sign', 'Integer')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (37, 0, 'Double')
     * */
    public static Integer sign_double(final Double ps) {
        if (ps == 0D)
            return 0;
        else if (ps > 0D)
            return 1;
        return -1;
    }

    /*
     * 正负判断
     *
     * insert into sql_to_java (id, method_name, return_type) values (38, 'sign', 'Integer')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (38, 0, 'Integer')
     * */
    public static Integer sign_int(final Integer ps) {
        if (ps == 0)
            return 0;
        else if (ps > 0)
            return 1;
        return -1;
    }

    public static Integer sign_long(final Long ps)
    {
        if (ps == 0L)
            return 0;
        else if (ps > 0L)
            return 1;
        return -1;
    }

    /*
     * 正负判断
     *
     * insert into sql_to_java (id, method_name, return_type) values (39, 'sign', 'Integer')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (39, 0, 'String')
     * */
    public static Integer sign_str(final String pss) {
        Integer ps = MyConvertUtil.ConvertToInt(pss);
        if (ps == 0)
            return 0;
        else if (ps > 0)
            return 1;
        return -1;
    }

    /**
     * 如果 trunc 里面是数字的情况
     *
     * insert into sql_to_java (id, method_name, return_type) values (40, 'trunc', 'Double')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (40, 0, 'Double')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (40, 1, 'Integer')
     */
    public static Double trunc_double(final Double ps, final Integer num) {
        String str = String.valueOf(ps);
        String front = str.split("\\.")[0];
        String back = str.split("\\.")[1];
        String result = front;
        if (num > 0 && num > back.length()) {
            return ps;
        }
        if (num < 0 && front.length() + num < 0) {
            return Double.valueOf(0);
        }
        if (num > 0) {
            result = front + "." + back.substring(0, num);
        } else {
            int length = front.length();
            String f = front.substring(0, length + num);
            for (int i = f.length(); i < length; i++) {
                f += "0";
            }
            result = f;
        }
        return Double.parseDouble(result);
    }

    /**
     * 如果 trunc 里面是时间的情况
     *
     * insert into sql_to_java (id, method_name, return_type) values (41, 'trunc', 'Date')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (41, 0, 'Date')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (41, 1, 'String')
     */
    public static Date trunc_date(final Date ps, final String format) {
        SimpleDateFormat sdf = null;
        switch (format.trim().toLowerCase()) {
            case "yyyy":
                sdf = new SimpleDateFormat(format);
                String year = sdf.format(ps);
                try {
                    sdf = new SimpleDateFormat("yyyy-MM-dd");
                    return sdf.parse(year + "-01-01");
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            case "mm":
                sdf = new SimpleDateFormat("yyyy-MM");
                String month = sdf.format(ps);
                try {
                    sdf = new SimpleDateFormat("yyyy-MM-dd");
                    return sdf.parse(month + "-01");
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            case "dd":
                sdf = new SimpleDateFormat("yyyy-MM-dd");
                String day = sdf.format(ps);
                try {
                    return sdf.parse(day);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
        }
        return null;
    }

    /*
    trunc, 参数为一个date时，默认按天处理
    insert into sql_to_java (id, method_name, return_type) values (42, 'trunc', 'Date')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (42, 0, 'Date')
     */
    public static Date trunc_single_date(final Date ps) {
        return trunc_date(ps, "dd");
    }

    /**
     * substr, start可能为负值
     * insert into sql_to_java (id, method_name, return_type) values (43, 'substr', 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (43, 0, 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (43, 1, 'Integer')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (43, 2, 'Integer')
     */
    public static String substr(final String str, final Integer start, final Integer count) {
        if (str != null && str.length() > 0) {
            if (start > 0) {
                return str.substring(start - 1, start + count - 1);
            } else if (start == 0) {
                return str.substring(0, start + count);
            } else {
                return str.substring(str.length() + start, str.length());
            }
        } else {
            return "";
        }
    }

    /*
     * ascii:返回ascii对码,按oracle标准,可接受字符串,此时返回第一个字母的码值
     *
     * insert into sql_to_java (id, method_name, return_type) values (44, 'ascii', 'Integer')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (44, 0, 'String')
     */
    public static Integer ascii(final String str) {
        if (str != null && str.length() > 0) {
            return (int) str.toCharArray()[0];
        } else {
            return null;
        }
    }

    /**
     * least()
     *
     * insert into sql_to_java (id, method_name, return_type) values (45, 'least', 'T')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (45, 0, 'T...')
     */
    public static <T extends Comparable & Serializable> T least(final T... t) {

        Optional<T> largest = Arrays.stream(t).min(T::compareTo);
        if (largest.isPresent()) {
            return largest.get();
        } else {
            return null;
        }
    }

    public static <T extends Comparable & Serializable> T least(final List<T> t) {

        Optional<T> largest = t.stream().min(T::compareTo);
        if (largest.isPresent()) {
            return largest.get();
        } else {
            return null;
        }
    }

    /**
     * greatest()
     *
     * insert into sql_to_java (id, method_name, return_type) values (46, 'greatest', 'T')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (46, 0, 'T...')
     */
    public static <T extends Comparable & Serializable> T greatest(final T... t) {

        Optional<T> largest = Arrays.stream(t).max(T::compareTo);
        if (largest.isPresent()) {
            return largest.get();
        } else {
            return null;
        }
    }

    public static <T extends Comparable & Serializable> T greatest(final List<T> t) {

        Optional<T> largest = t.stream().max(T::compareTo);
        if (largest.isPresent()) {
            return largest.get();
        } else {
            return null;
        }
    }

    /**
     * months_between(),oracle可返回小数，但未发现对小数四舍五入的情况，由于java只能按每月平均30.2天处理，可能产生结果误差，综上，此处返回整数,可能为负数
     *
     * insert into sql_to_java (id, method_name, return_type) values (47, 'months_between', 'Integer')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (47, 0, 'Date')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (47, 1, 'Date')
     * */
    public static Integer months_between(Date end, Date start) {
        int flag = 1;
        if (start == null || end == null) {
            return null;
        }
        if (start.after(end)) {
            Date t = start;
            start = end;
            end = t;
            flag = -1;
        }
        int gap = 0;
        Calendar startCalendar = Calendar.getInstance();
        startCalendar.setTime(start);
        Calendar endCalendar = Calendar.getInstance();
        endCalendar.setTime(end);
        Calendar temp = Calendar.getInstance();
        temp.setTime(end);
        temp.add(Calendar.DATE, 1);
        int year = endCalendar.get(Calendar.YEAR) - startCalendar.get(Calendar.YEAR);
        int month = endCalendar.get(Calendar.MONTH) - startCalendar.get(Calendar.MONTH);
        if ((startCalendar.get(Calendar.DATE) == 1) && (temp.get(Calendar.DATE) == 1)) {
            gap = year * 12 + month + 1;
        } else if ((startCalendar.get(Calendar.DATE) != 1) && (temp.get(Calendar.DATE) == 1)) {
            gap = year * 12 + month;
        } else if ((startCalendar.get(Calendar.DATE) == 1) && (temp.get(Calendar.DATE) != 1)) {
            gap = year * 12 + month;
        } else {
            gap = (year * 12 + month - 1) < 0 ? 0 : (year * 12 + month);
        }
        return gap * flag;
    }

    /**
     * instr(), java的indexof,返回字符或字符串所在位置
     *
     * insert into sql_to_java (id, method_name, return_type) values (48, 'instr', 'Integer')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (48, 0, 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (48, 1, 'String')
     */
    public static Integer instr(String str, String key) {
        if (!Strings.isNullOrEmpty(str) && !Strings.isNullOrEmpty(key)) {
            return str.indexOf(key) + 1;
        }
        return null;
    }

    /**
     * replace(),with 3 parameters
     *
     * insert into sql_to_java (id, method_name, return_type) values (49, 'replace', 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (49, 0, 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (49, 1, 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (49, 2, 'String')
     * */
    public static String replace(String str, String key, String value) {

        if (!Strings.isNullOrEmpty(str) && !Strings.isNullOrEmpty(key) && !Strings.isNullOrEmpty(value))
        {
            return str.replaceAll(key, value);
        }

        return "";
    }

    /**
     * replace(),with 2 parameters
     *
     * insert into sql_to_java (id, method_name, return_type) values (50, 'replace', 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (50, 0, 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (50, 1, 'String')
     * */
    public static String replace(String str, String key) {

        if (!Strings.isNullOrEmpty(str) && !Strings.isNullOrEmpty(key))
        {
            return str.replaceAll(key, "");
        }
        return "";
    }

    /**
     * 向上取整
     * @param v
     * @return
     *
     * insert into sql_to_java (id, method_name, return_type) values (51, 'ceil', 'Integer')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (51, 0, 'Double')
     */
    public static Integer ceil(Double v){
        return (int)Math.ceil(v);
    }

    /**
     * 向下取整
     * @param v
     * @return
     *
     * insert into sql_to_java (id, method_name, return_type) values (52, 'floor', 'Integer')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (52, 0, 'Double')
     */
    public static Integer floor(Double v){
        return (int)Math.floor(v);
    }

    /**
     * 取余数
     * @param v1
     * @param v2
     * @return
     *
     * insert into sql_to_java (id, method_name, return_type) values (53, 'mod', 'Integer')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (53, 0, 'Integer')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (53, 1, 'Integer')
     */
    public static Integer mod(Integer v1,Integer v2) {
        if (v2 == 0) {
            return v1;
        }
        return v1 % v2;
    }

    /**
     * 指定进度（num）对v进行四舍五入
     * @param v0
     * @param num
     * @return
     *
     * insert into sql_to_java (id, method_name, return_type) values (54, 'round', 'Double')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (54, 0, 'Double')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (54, 1, 'Integer')
     */
    public static Double round(Double v0, Integer num) {
        double v1 = (double)num;
        double f = v1 == 0D ? 1. : Math.pow(10., v1);
        double middleResult = v0 * f + 0.001D;

        if (Double.MAX_VALUE > middleResult && Double.MIN_VALUE < v1) {
            //double v1 = (double)num;
            //double f = v1 == 0D ? 1. : Math.pow(10., v1);
            //double middleResult = v0 * f + 0.001D;

            int oneWithSymbol = middleResult > 0 ? 1 : -1;
            double result = Math.round(Math.abs(middleResult)) / f * oneWithSymbol;

            return result;
        }
        else {
            BigDecimal value = new BigDecimal(Double.toString(v0));
            return value.setScale(num, BigDecimal.ROUND_HALF_UP).doubleValue();
        }
    }

    /**
     * 字符串转小写
     * @param str
     * @return
     *
     * insert into sql_to_java (id, method_name, return_type) values (55, 'lower', 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (55, 0, 'String')
     */
    public static String lower(String str){
        if(str==null){
            return null;
        }
        return str.toLowerCase();
    }

    /**
     * 字符串转大写
     * @param str
     * @return
     *
     * insert into sql_to_java (id, method_name, return_type) values (56, 'upper', 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (56, 0, 'String')
     */
    public static String upper(String str){
        if(str==null){
            return null;
        }
        return str.toUpperCase();
    }

    /**
     * 从字符串中指定的开始位置，取得指定字节数的字符串
     * @param str
     * @param begin
     * @param length
     * @return
     *
     * insert into sql_to_java (id, method_name, return_type) values (57, 'substrb', 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (57, 0, 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (57, 1, 'Integer')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (57, 2, 'Integer')
     */
    public static String substrb(String str, Integer begin, Integer length) {
        byte[] src = str.getBytes(Charset.forName("GBK"));//与oracle编码设置为一样
        if (begin > 0) {
            begin--;
        }
        if (begin < 0) {
            begin = src.length + begin;
        }
        byte[] dest = new byte[src.length];
        int destPos = 0;
        for (int i = begin; i < src.length && i < begin + length && begin > 0; ) {
            if ((src[i] < 0 || src[i] > 127) && (i + 1 < begin + length) && (src[i + 1] < 0 || src[i + 1] > 127)) {
                System.arraycopy(src, i, dest, destPos, 2);
                destPos += 2;
                i += 2;
            } else if ((src[i] < 0 || src[i] > 127) && (i + 1 < begin + length) && (src[i + 1] >= 0 && src[i + 1] <= 127)) {
                i++;
            } else if ((src[i] < 0 || src[i] > 127) && (i + 1 >= begin + length)) {
                i++;
            } else {
                System.arraycopy(src, i, dest, destPos, 1);
                destPos++;
                i++;
            }
        }
        try {
            return new String(dest, "GBK");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * 获取字符串字节数
     * @param str
     * @return
     *
     * insert into sql_to_java (id, method_name, return_type) values (58, 'lengthb', 'Integer')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (58, 0, 'String')
     */
    public static Integer lengthb(String str){
        try {
            return str.getBytes("GBK").length;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * ascii码转字符
     * @param ascii
     * @return
     *
     * insert into sql_to_java (id, method_name, return_type) values (59, 'chr', 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (59, 0, 'Integer')
     */
    public static String chr(Integer ascii){
        return String.valueOf((char)ascii.intValue());
    }

    /*
     * 返回字节所在位置
     *
     * insert into sql_to_java (id, method_name, return_type) values (60, 'instrb', 'Integer')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (60, 0, 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (60, 1, 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (60, 2, 'Integer')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (60, 3, 'Integer')
     * */
    public static Integer instrb(String src, String dest, Integer begin, Integer time) {
        try {
            byte[] objBytes = src.getBytes("GBK");
            if (begin < 1 || begin > objBytes.length || time <= 0) {
                return 0;
            }
            begin--;
            byte[] destBytes = dest.getBytes("GBK");
            byte[] srcBytes = new byte[objBytes.length - begin];
            System.arraycopy(objBytes, begin, srcBytes, 0, objBytes.length - begin);
            Map<Integer, Integer> timeMap = new HashMap<>();
            int t = 1;
            for (int i = 0; i < srcBytes.length; i++) {
                byte src_byte = srcBytes[i];
                if (src_byte == destBytes[0]) {
                    boolean flag = true;
                    for (int j = 1; j < destBytes.length; j++) {
                        if (destBytes[j] != srcBytes[i + j]) {
                            flag = false;
                            break;
                        }
                    }
                    if (flag) {
                        timeMap.put(t++, i + 1 + begin);
                    }
                }
            }
            return timeMap.get(time) == null ? 0 : timeMap.get(time);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return 0;
    }

    /*
     * 默认去空格
     *
     * insert into sql_to_java (id, method_name, return_type) values (61, 'ltrim', 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (61, 0, 'String')
     * */
    public static String ltrim(String str){
        return ltrim(str," ");
    }

    /**
     * 过滤指定字符
     *
     * insert into sql_to_java (id, method_name, return_type) values (62, 'ltrim', 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (62, 0, 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (62, 1, 'String')
     * */
    public static String ltrim(String str, String key){
        StringBuilder sb = new StringBuilder();
        char[] src = str.toCharArray();
        int index=0;
        for(int i=0;i<src.length;i++){
            index=i;
            if(key.indexOf(src[i])<0){
                break;
            }
        }
        for(int i=index;i<src.length;i++){
            sb.append(src[i]);
        }
        return sb.toString();
    }

    /*
     * 去字符串右边的空格
     *
     * insert into sql_to_java (id, method_name, return_type) values (63, 'rtrim', 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (63, 0, 'String')
     * */
    public static String rtrim(String str){
        return rtrim(str," ");
    }

    /*
     * 去字符串右边指定的字符串
     *
     * insert into sql_to_java (id, method_name, return_type) values (64, 'rtrim', 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (64, 0, 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (64, 1, 'String')
     *
     * */
    public static String rtrim(String str, String key){
        StringBuilder sb = new StringBuilder();
        char[] src = str.toCharArray();
        int index=0;
        for(int i=src.length-1;i>0;i--){
            index=i;
            if(key.indexOf(src[i])<0){
                break;
            }
        }
        for(int i=0;i<index+1;i++){
            sb.append(src[i]);
        }
        return sb.toString();
    }

    /*
     * 删除左右空格
     *
     * insert into sql_to_java (id, method_name, return_type) values (65, 'trim', 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (65, 0, 'String')
     * */
    public static String trim(String str){
        return str.trim();
    }

    /**
     * insert into sql_to_java (id, method_name, return_type) values (66, 'translate', 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (66, 0, 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (66, 1, 'String')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (66, 2, 'String')
     * */
//    public static String translate(String str, String from_str, String to_str){
//        if(str==null || from_str==null || to_str==null || to_str.equals("") || from_str.equals("") || str.equals("")){
//            return null;
//        }
//        for (int i=0;i<from_str.length();i++){
//            if(i<to_str.length()){
//                str = str.replaceAll(String.valueOf(from_str.charAt(i)),String.valueOf(to_str.charAt(i)));
//            }else{
//                str = str.replaceAll(String.valueOf(from_str.charAt(i)),"");
//            }
//        }
//        return str;
//    }

    /**
     * nvl 的内置函数
     *
     * insert into sql_to_java (id, method_name, return_type) values (67, 'nvl', 'V')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (67, 0, 'Object')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (67, 1, 'Object')
     * */
    public static <V> V nvl(Object obj, Object default_vs)
    {
        if (obj == null)
            return (V)default_vs;
        return (V)obj;
    }

    private static double my_roundMagic(double d) {
        if ((d < 0.000_000_000_000_1) && (d > -0.000_000_000_000_1)) {
            return 0.0;
        }
        if ((d > 1_000_000_000_000d) || (d < -1_000_000_000_000d)) {
            return d;
        }
        StringBuilder s = new StringBuilder();
        s.append(d);
        if (s.toString().indexOf('E') >= 0) {
            return d;
        }
        int len = s.length();
        if (len < 16) {
            return d;
        }
        if (s.toString().indexOf('.') > len - 3) {
            return d;
        }
        s.delete(len - 2, len);
        len -= 2;
        char c1 = s.charAt(len - 2);
        char c2 = s.charAt(len - 3);
        char c3 = s.charAt(len - 4);
        if ((c1 == '0') && (c2 == '0') && (c3 == '0')) {
            s.setCharAt(len - 1, '0');
        } else if ((c1 == '9') && (c2 == '9') && (c3 == '9')) {
            s.setCharAt(len - 1, '9');
            s.append('9');
            s.append('9');
            s.append('9');
        }
        return Double.parseDouble(s.toString());
    }

    /**
     * avg 平均数
     *
     * insert into sql_to_java (id, method_name, return_type) values (68, 'avg', 'Object')
     * insert into sql_to_java_ps (sql_to_java_id, ps_index, ps_type) values (68, 0, 'Object...')
     * */
    public static <T extends Number> Double avg(final T ... ps)
    {
        return Arrays.stream(ps).mapToDouble(i -> i.doubleValue()).average().getAsDouble();
    }

    public static <T extends Number> Double avg(final List<T> ps)
    {
        return ps.stream().mapToDouble(i -> i.doubleValue()).average().getAsDouble();
    }

    public static Long bitand(Long v0, Long v1)
    {
        return v0 & v1;
    }

    public static Boolean bitget(Long v0, Integer v1)
    {
        return (v0 & (1L << v1)) != 0;
    }

    public static Long bitor(Long v0, Long v1)
    {
        return v0 | v1;
    }

    public static Long bitxor(Long v0, Long v1)
    {
        return v0 ^ v1;
    }

    public static String random_uuid()
    {
        return ValueUuid.getNewRandom().toString();
    }

    public static String insert(String s1, int start, int length, String s2)
    {
        return Function.insert(s1, start, length, s2);
    }

    public static String left(String s, int count)
    {
        return Function.left(s, count);
    }

    public static String right(String s, int count)
    {
        return Function.right(s, count);
    }

    public static int locate(String search, String s, int start)
    {
        return Function.locate(search, s, start);
    }

    public static int position(String search, String s)
    {
        return Function.locate(search, s, 0);
    }

    public static String soundex(String s)
    {
        return Function.getSoundex(s);
    }

    public static String space(int v0)
    {
        int len = Math.max(0, v0);
        char[] chars = new char[len];
        for (int i = len - 1; i >= 0; i--) {
            chars[i] = ' ';
        }
        return new String(chars);
    }

    public static String stringencode(String s)
    {
        return StringUtils.javaDecode(s);
    }

    public static byte[] stringtoutf8(String s)
    {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static String translate(String original, String findChars, String replaceChars)
    {
        return Function.translate(original, findChars, replaceChars);
    }

    public static Date current_date()
    {
        Value vs = DateTimeUtils.timestampTimeZoneFromMillis(System.currentTimeMillis()).convertTo(Value.DATE);
        return vs.getDate();
    }

    public static Time current_time()
    {
        ValueTime vt = (ValueTime) DateTimeUtils.timestampTimeZoneFromMillis(System.currentTimeMillis()).convertTo(Value.TIME);
        return vt.convertScale(false, 0).getTime();
    }

    public static Timestamp current_timestamp()
    {
        ValueTimestampTimeZone vt = (ValueTimestampTimeZone) DateTimeUtils.timestampTimeZoneFromMillis(System.currentTimeMillis());
        return vt.convertScale(false, 6).convertTo(Value.TIMESTAMP).getTimestamp();
    }

    public static Timestamp myAdd(String flag, long count, String date)
    {
        ValueTimestamp ts = ValueTimestamp.parse(date);
        return DateTimeFunctions.dateadd(flag, count, ts).getTimestamp();
    }

    public static Long myDiff(String flag, String v1, String v2)
    {
        ValueTimestamp ts = ValueTimestamp.parse(v1);
        ValueTimestamp ts2 = ValueTimestamp.parse(v2);

        return DateTimeFunctions.datediff(flag, ts, ts2);
    }

    /**
     * 添加年度
     * */
    public static Timestamp add_year(long count, String v)
    {
//        ValueTimestamp ts2 = ValueTimestamp.parse(v);
//        ValueDate vt = (ValueDate) ts2.convertTo(Value.DATE);
//
//        return DateTimeFunctions.dateadd("YEAR", count, vt).getDate();
        return myAdd("YEAR", count, v);
    }

    /**
     * 添加季度
     * */
    public static Timestamp add_quarter(long count, String v)
    {
//        ValueTimestamp ts2 = ValueTimestamp.parse(v);
//        ValueDate vt = (ValueDate) ts2.convertTo(Value.DATE);
//
//        return DateTimeFunctions.dateadd("QUARTER", count, vt).getDate();
        return myAdd("QUARTER", count, v);
    }

    /**
     * 添加月份
     * */
    public static Timestamp add_month(long count, String v)
    {
//        ValueTimestamp ts2 = ValueTimestamp.parse(v);
//        ValueDate vt = (ValueDate) ts2.convertTo(Value.DATE);
//
//        return DateTimeFunctions.dateadd("MONTH", count, vt).getDate();
        return myAdd("MONTH", count, v);
    }

    /**
     * 添加日期
     * */
    public static Timestamp add_date(long count, String v)
    {
//        ValueTimestamp ts2 = ValueTimestamp.parse(v);
//        ValueDate vt = (ValueDate) ts2.convertTo(Value.DATE);
//
//        return DateTimeFunctions.dateadd("DAY", count, vt).getDate();
        return myAdd("DAY", count, v);
    }

    /**
     * 添加小时
     * */
    public static Timestamp add_hour(long count, String v)
    {
//        ValueTimestamp ts2 = ValueTimestamp.parse(v);
//        ValueDate vt = (ValueDate) ts2.convertTo(Value.DATE);
//
//        return DateTimeFunctions.dateadd("HOUR", count, vt).getDate();
        return myAdd("HOUR", count, v);
    }

    /**
     * 添加秒
     * */
    public static Timestamp add_second(long count, String v)
    {
        return myAdd("SECOND", count, v);
    }

    /**
     * 添加毫秒
     * */
    public static Timestamp add_ms(long count, String v)
    {
        return myAdd("MS", count, v);
    }

    /**
     * 相差年度
     * */
    public static Long diff_year(String v0, String v1)
    {
        return myDiff("YEAR", v0, v1);
    }

    /**
     * 添加季度
     * */
    public static Long diff_quarter(String v0, String v1)
    {
        //return myAdd("QUARTER", count, v);
        return myDiff("QUARTER", v0, v1);
    }

    /**
     * 添加月份
     * */
    public static Long diff_month(String v0, String v1)
    {
        return myDiff("MONTH", v0, v1);
    }

    /**
     * 添加日期
     * */
    public static Long diff_date(String v0, String v1)
    {
        return myDiff("DAY", v0, v1);
    }

    /**
     * 添加小时
     * */
    public static Long diff_hour(String v0, String v1)
    {
        return myDiff("HOUR", v0, v1);
    }

    /**
     * 添加秒
     * */
    public static Long diff_second(String v0, String v1)
    {
        return myDiff("SECOND", v0, v1);
    }

    /**
     * 添加毫秒
     * */
    public static Long diff_ms(String v0, String v1)
    {
        return myDiff("MS", v0, v1);
    }

    public static String dayname(String v)
    {
        int dayOfWeek = DateTimeUtils.getSundayDayOfWeek(DateTimeUtils.dateAndTimeFromValue(ValueTimestamp.parse(v))[0]);
        return DateTimeFunctions.getMonthsAndWeeks(1)[dayOfWeek];
    }

    public static long day_of_month(String v)
    {
        ValueTimestamp ts = ValueTimestamp.parse(v);
        return DateTimeUtils.dayFromDateValue(ts.getDateValue());
    }

    public static long day_of_week(String v)
    {
        ValueTimestamp ts = ValueTimestamp.parse(v);
        return DateTimeUtils.getSundayDayOfWeek(ts.getDateValue());
    }

    public static long day_of_year(String v)
    {
        ValueTimestamp ts = ValueTimestamp.parse(v);
        return DateTimeUtils.getDayOfYear(ts.getDateValue());
    }

    public static int hour(String v)
    {
        ValueTimestamp ts = ValueTimestamp.parse(v);
        java.sql.Date date = new java.sql.Date(ts.getTimestamp().getTime());
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.HOUR_OF_DAY);
    }

    public static int minute(String v)
    {
        ValueTimestamp ts = ValueTimestamp.parse(v);
        java.sql.Date date = new java.sql.Date(ts.getTimestamp().getTime());
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.MINUTE);
    }

    public static String monthname(String v)
    {
        int month = DateTimeUtils.monthFromDateValue(DateTimeUtils.dateAndTimeFromValue(ValueTimestamp.parse(v))[0]);
        return DateTimeFunctions.getMonthsAndWeeks(0)[month - 1];
    }

    public static int quarter(String v)
    {
        ValueTimestamp ts = ValueTimestamp.parse(v);
        return (DateTimeUtils.monthFromDateValue(ts.getDateValue()) - 1) / 3 + 1;
    }

    public static int second(String v)
    {
        ValueTimestamp ts = ValueTimestamp.parse(v);
        java.sql.Date date = new java.sql.Date(ts.getTimestamp().getTime());
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.SECOND);
    }

    public static int week(String v)
    {
        ValueTimestamp ts = ValueTimestamp.parse(v);
        Date date = new Date(ts.getTimestamp().getTime());
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.WEEK_OF_YEAR);
    }

    public static int year(String v)
    {
        ValueTimestamp ts = ValueTimestamp.parse(v);
        return DateTimeUtils.yearFromDateValue(ts.getDateValue());
    }

    public static int month(String v)
    {
        ValueTimestamp ts = ValueTimestamp.parse(v);
        return DateTimeUtils.monthFromDateValue(ts.getDateValue());
    }
}

































