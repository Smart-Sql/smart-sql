package org.tools;

import cn.plus.model.db.MyScenesParams;
import cn.plus.model.ddl.MyFunc;
import cn.plus.model.ddl.MyFuncPs;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.query.h2.ConnectionManager;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.schedule.IgniteScheduleProcessor;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * plus 的工具类
 * */
public class MyPlusUtil {

    /**
     * 输入 ignite 获取 GridKernalContext
     * @param ignite 输入 ignite 对象
     * @return ignite 的 GridKernalContext
     * */
    public static GridKernalContext getGridKernalContext(final Ignite ignite)
    {
        return ((IgniteKernal) ignite).context();
    }

    /**
     * 输入 ignite 获取 ConnectionManager 对象
     * */
    public static ConnectionManager getConnMgr(final Ignite ignite)
    {
        GridKernalContext ctx = ((IgniteEx)ignite).context();
        IgniteH2Indexing h2Indexing = (IgniteH2Indexing)ctx.query().getIndexing();
        ConnectionManager connMgr = h2Indexing.connections();
        return connMgr;
    }

    /**
     * 输入 ignite 获取 IgniteH2Indexing 对象
     * */
    public static IgniteH2Indexing getIgniteH2Indexing(final Ignite ignite)
    {
        GridKernalContext ctx = ((IgniteEx)ignite).context();
        return (IgniteH2Indexing)ctx.query().getIndexing();
    }

    /**
     * 获取 IgniteScheduleProcessor
     * */
    public static IgniteScheduleProcessor getIgniteScheduleProcessor(final Ignite ignite)
    {
        return (IgniteScheduleProcessor)getGridKernalContext(ignite).schedule();
    }

    /**
     * 通过参数类型的名称获取参数类型
     * */
    private static Class<?> getClassByName(final String typeName)
    {
        switch (typeName)
        {
            case "String":
                return String.class;
            case "Double":
                return Double.class;
            case "double":
                return double.class;
            case "Integer":
                return Integer.class;
            case "int":
                return int.class;
            case "Long":
                return Long.class;
            case "long":
                return long.class;
            case "Boolean":
                return Boolean.class;
            case "boolean":
                return boolean.class;
            case "Date":
                return Date.class;
            case "Timestamp":
                return Timestamp.class;
        }

        return null;
    }

    public static String getValue(final String vs, final String ps_type) throws Exception {
        switch (ps_type)
        {
            case "Double":
                return String.format("(MyConvertUtil/ConvertToDouble %s)", vs);
            case "double":
                return String.format("(MyConvertUtil/ConvertToDouble %s)", vs);
            case "Integer":
                return String.format("(MyConvertUtil/ConvertToInt %s)", vs);
            case "int":
                return String.format("(MyConvertUtil/ConvertToInt %s)", vs);
            case "Long":
                return String.format("(MyConvertUtil/ConvertToLong %s)", vs);
            case "long":
                return String.format("(MyConvertUtil/ConvertToLong %s)", vs);
            case "Boolean":
                return String.format("(MyConvertUtil/ConvertToBoolean %s)", vs);
            case "boolean":
                return String.format("(MyConvertUtil/ConvertToBoolean %s)", vs);
            case "Date":
                return String.format("(MyConvertUtil/ConvertToTimestamp %s)", vs);
            case "Timestamp":
                return String.format("(MyConvertUtil/ConvertToTimestamp %s)", vs);
            default:
                return String.format("(MyConvertUtil/ConvertToString %s)", vs);
        }
    }

    public static List<MyScenesParams> getParams(final List<MyScenesParams> params)
    {
        return params.stream().sorted((a, b) -> a.getPs_index() - b.getPs_index()).collect(Collectors.toList());
    }

    /**
     * 调用 func
     * */
    public static Object invokeFuncObj(final Ignite ignite, final String func_name, Object... ps)
    {
        Object rs = null;
        IgniteCache<String, MyFunc> funcCache = ignite.cache("my_func");
        MyFunc myFunc = funcCache.get(func_name);
        try {

            List<MyFuncPs> lstFunc = myFunc.getLst().stream().sorted((a, b) -> a.getPs_index() - b.getPs_index()).collect(Collectors.toList());
            List<Class<?>> pstypes = lstFunc.stream().map(m -> getClassByName(m.getPs_type())).collect(Collectors.toList());

            List<Object> psvalue = new ArrayList<>();
            for (int i = 0; i < ps.length; i++)
            {
                MyFuncPs myFuncPs = lstFunc.get(i);
                switch (myFuncPs.getPs_type())
                {
                    case "String":
                        if (ps[i] instanceof String) {
                            psvalue.add(ps[i]);
                        }
                        else
                        {
                            psvalue.add(MyConvertUtil.ConvertToString(ps[i]));
                        }
                        break;
                    case "Double":
                        if (ps[i] instanceof Double)
                        {
                            psvalue.add(ps[i]);
                        }
                        else {
                            psvalue.add(MyConvertUtil.ConvertToDouble(ps[i]));
                        }
                        break;
                    case "double":
                        psvalue.add(MyConvertUtil.ConvertToDouble(ps[i]));
                        break;
                    case "Integer":
                        if (ps[i] instanceof Integer)
                        {
                            psvalue.add(ps[i]);
                        }
                        else {
                            psvalue.add(MyConvertUtil.ConvertToInt(ps[i]));
                        }
                        break;
                    case "int":
                        psvalue.add(MyConvertUtil.ConvertToInt(ps[i]));
                        break;
                    case "Long":
                        if (ps[i] instanceof Long)
                        {
                            psvalue.add(ps[i]);
                        }
                        else {
                            psvalue.add(MyConvertUtil.ConvertToLong(ps[i]));
                        }
                        break;
                    case "long":
                        psvalue.add(MyConvertUtil.ConvertToLong(ps[i]));
                        break;
                    case "Boolean":
                        if (ps[i] instanceof Boolean)
                        {
                            psvalue.add(ps[i]);
                        }
                        else {
                            psvalue.add(MyConvertUtil.ConvertToBoolean(ps[i]));
                        }
                        break;
                    case "boolean":
                        psvalue.add(MyConvertUtil.ConvertToBoolean(ps[i]));
                        break;
                    case "Date":
                        if (ps[i] instanceof Date)
                        {
                            psvalue.add(ps[i]);
                        }
                        else {
                            psvalue.add(MyConvertUtil.ConvertToTimestamp(ps[i]));
                        }
                        break;
                    case "Timestamp":
                        if (ps[i] instanceof Timestamp)
                        {
                            psvalue.add(ps[i]);
                        }
                        else {
                            psvalue.add(MyConvertUtil.ConvertToTimestamp(ps[i]));
                        }
                        break;
                    default:
                        psvalue.add(ps[i]);
                        break;
                }
            }

            Class<?> cls = Class.forName(myFunc.getCls_name());
            Method method = cls.getMethod(myFunc.getJava_method_name(), pstypes.toArray(new Class[]{}));
            rs = method.invoke(cls.newInstance(), psvalue.toArray());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rs;
    }

    /**
     * 调用 func
     * */
    public static Object invokeFunc(final Ignite ignite, final String func_name, String... ps)
    {
        Object rs = null;
        IgniteCache<String, MyFunc> funcCache = ignite.cache("my_func");
        MyFunc myFunc = funcCache.get(func_name);
        try {

            List<MyFuncPs> lstFunc = myFunc.getLst().stream().sorted((a, b) -> a.getPs_index() - b.getPs_index()).collect(Collectors.toList());
            List<Class<?>> pstypes = lstFunc.stream().map(m -> getClassByName(m.getPs_type())).collect(Collectors.toList());

            List<Object> psvalue = new ArrayList<>();
            for (int i = 0; i < ps.length; i++)
            {
                MyFuncPs myFuncPs = lstFunc.get(i);
                switch (myFuncPs.getPs_type())
                {
                    case "String":
                        psvalue.add(ps[i]);
                        break;
                    case "Double":
                        psvalue.add(MyConvertUtil.ConvertToDouble(ps[i]));
                        break;
                    case "double":
                        psvalue.add(MyConvertUtil.ConvertToDouble(ps[i]));
                        break;
                    case "Integer":
                        psvalue.add(MyConvertUtil.ConvertToInt(ps[i]));
                        break;
                    case "int":
                        psvalue.add(MyConvertUtil.ConvertToInt(ps[i]));
                        break;
                    case "Long":
                        psvalue.add(MyConvertUtil.ConvertToLong(ps[i]));
                        break;
                    case "long":
                        psvalue.add(MyConvertUtil.ConvertToLong(ps[i]));
                        break;
                    case "Boolean":
                        psvalue.add(MyConvertUtil.ConvertToBoolean(ps[i]));
                        break;
                    case "boolean":
                        psvalue.add(MyConvertUtil.ConvertToBoolean(ps[i]));
                        break;
                    case "Date":
                        psvalue.add(MyConvertUtil.ConvertToTimestamp(ps[i]));
                        break;
                    case "Timestamp":
                        psvalue.add(MyConvertUtil.ConvertToTimestamp(ps[i]));
                        break;
                    default:
                        psvalue.add(ps[i]);
                        break;
                }
            }

            Class<?> cls = Class.forName(myFunc.getCls_name());
            Method method = cls.getMethod(myFunc.getJava_method_name(), pstypes.toArray(new Class[]{}));
            rs = method.invoke(cls.newInstance(), psvalue.toArray());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rs;
    }

    public static Object invokeFuncNoPs(final Ignite ignite, final String func_name)
    {
        Object rs = null;
        IgniteCache<String, MyFunc> funcCache = ignite.cache("my_func");
        MyFunc myFunc = funcCache.get(func_name);
        try {


            Class<?> cls = Class.forName(myFunc.getCls_name());
            Method method = cls.getMethod(myFunc.getJava_method_name());
            rs = method.invoke(cls.newInstance());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rs;
    }
}















































