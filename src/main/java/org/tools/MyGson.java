package org.tools;

import clojure.lang.PersistentVector;
import cn.plus.model.ddl.MyFuncPs;
import cn.plus.model.ddl.MyUserFunc;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

public class MyGson {
    private static Gson gson = new GsonBuilder()
            .enableComplexMapKeySerialization()
            .setDateFormat("yyyy-MM-dd HH:mm:ss")
            .create();

    public static Hashtable<String, Object> getHashtable(final String line)
    {
        return gson.fromJson(line, new TypeToken<Hashtable<String, Object>>(){}.getType());
    }

    public static String groupObjToLine(final PersistentVector vs)
    {
        return gson.toJson(vs);
    }

    public static String groupObjToLine(final ArrayList vs)
    {
        return gson.toJson(vs);
    }

    public static String groupObjToLine(final Object vs)
    {
        return gson.toJson(vs);
    }

    public static List<Object> lineToObj(final String line)
    {
        return gson.fromJson(line, new TypeToken<List<Object>>(){}.getType());
    }

    public static MyUserFunc getUserFunc(final String userFunc)
    {
        return gson.fromJson(userFunc, new TypeToken<MyUserFunc>(){}.getType());
    }

    public static MyUserFunc getUserFunc(final Hashtable<String, Object> userFunc)
    {
        MyUserFunc m = new MyUserFunc();
        if (userFunc.containsKey("method_name"))
        {
            m.setMethod_name(userFunc.get("method_name").toString());
        }
        if (userFunc.containsKey("java_method_name"))
        {
            m.setJava_method_name(userFunc.get("java_method_name").toString());
        }
        if (userFunc.containsKey("cls_name"))
        {
            m.setCls_name(userFunc.get("cls_name").toString());
        }
        if (userFunc.containsKey("return_type"))
        {
            m.setReturn_type(userFunc.get("return_type").toString());
        }
        if (userFunc.containsKey("descrip"))
        {
            m.setDescrip(userFunc.get("descrip").toString());
        }
        if (userFunc.containsKey("lst"))
        {
            List<MyFuncPs> lst_ps = new ArrayList<>();
            List<Hashtable<String, Object>> lst = (List<Hashtable<String, Object>>) userFunc.get("lst");
            for (Hashtable<String, Object> my_ht : lst)
            {
                lst_ps.add(new MyFuncPs(MyConvertUtil.ConvertToInt(my_ht.get("ps_index")), my_ht.get("ps_type").toString()));
            }
            m.setLst(lst_ps);
        }
        return m;
    }
}






























