package org.tools;

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

    /**
     * 调用 func
     * */
    public static Object invokeFunc(final Ignite ignite, final String func_name, Object... ps)
    {
        Object rs = null;
        IgniteCache<String, MyFunc> funcCache = ignite.cache("my_func");
        MyFunc myFunc = funcCache.get(func_name);
        try {
            List<Object> psvalue = new ArrayList<>();
            for (Object p : ps)
            {
                psvalue.add(p);
            }

            List<MyFuncPs> lstFunc = myFunc.getLst().stream().sorted((a, b) -> a.getPs_index() - b.getPs_index()).collect(Collectors.toList());
            List<Class<?>> pstypes = lstFunc.stream().map(m -> getClassByName(m.getPs_type())).collect(Collectors.toList());

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
        }
        return rs;
    }
}















































