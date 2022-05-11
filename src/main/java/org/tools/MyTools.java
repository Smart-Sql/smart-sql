package org.tools;

import clojure.lang.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MyTools {

    private static Gson gson = new GsonBuilder()
            .enableComplexMapKeySerialization()
            .setDateFormat("yyyy-MM-dd HH:mm:ss")
            .create();

    /**
     * 如果 line 是字典类型
     * 会转换恒 LinkedTreeMap
     * 如果是 line 是数组
     * 会转换成 ArrayList
     * */
    public static Object toObject(final String line)
    {
        return gson.fromJson(line, Object.class);
    }

    public static String toString(final Object object)
    {
        return gson.toJson(object);
    }

    public static Object[] toArrayObject(final PersistentVector vector)
    {
        List<Object> lst = new ArrayList<>();
        for (Object m : vector.toArray())
        {
            if (m instanceof APersistentMap)
            {
                lst.add(toHashtable((APersistentMap)m));
            }
            else if (m instanceof PersistentVector)
            {
                lst.add((Object[])toArrayObject((PersistentVector) m));
            }
            else if (m instanceof PersistentList)
            {
                lst.add((Object[])toArrayObject((PersistentList) m));
            }
            else if (m instanceof APersistentSet)
            {
                lst.add((Object[])toArrayObject((APersistentSet) m));
            }
            else
            {
                lst.add(m);
            }
        }
        return lst.toArray();
    }

    public static Object[] toArrayObject(final PersistentList vector)
    {
        List<Object> lst = new ArrayList<>();
        for (Object m : vector.toArray())
        {
            if (m instanceof APersistentMap)
            {
                lst.add(toHashtable((APersistentMap)m));
            }
            else if (m instanceof PersistentVector)
            {
                lst.add((Object[])toArrayObject((PersistentVector) m));
            }
            else if (m instanceof PersistentList)
            {
                lst.add((Object[])toArrayObject((PersistentList) m));
            }
            else if (m instanceof APersistentSet)
            {
                lst.add((Object[])toArrayObject((APersistentSet) m));
            }
            else
            {
                lst.add(m);
            }
        }
        return lst.toArray();
    }

    public static Object[] toArrayObject(final APersistentSet vector)
    {
        List<Object> lst = new ArrayList<>();
        for (Object m : vector.toArray())
        {
            if (m instanceof APersistentMap)
            {
                lst.add(toHashtable((APersistentMap)m));
            }
            else if (m instanceof PersistentVector)
            {
                lst.add((Object[])toArrayObject((PersistentVector) m));
            }
            else if (m instanceof PersistentList)
            {
                lst.add((Object[])toArrayObject((PersistentList) m));
            }
            else if (m instanceof APersistentSet)
            {
                lst.add((Object[])toArrayObject((APersistentSet) m));
            }
            else
            {
                lst.add(m);
            }
        }
        return lst.toArray();
    }

    public static Hashtable toHashtable(final APersistentMap map)
    {
        Hashtable ht = new Hashtable();
        if (map != null)
        {
            for (Object key : map.keySet())
            {
                if (map.get(key) instanceof APersistentMap)
                {
                    ht.put(key.toString().substring(1), toHashtable((APersistentMap)map.get(key)));
                }
                else if (map.get(key) instanceof PersistentVector)
                {
                    ht.put(key.toString().substring(1), (Object[])toArrayObject((PersistentVector) map.get(key)));
                }
                else if (map.get(key) instanceof PersistentList)
                {
                    ht.put(key.toString().substring(1), (Object[])toArrayObject((PersistentList) map.get(key)));
                }
                else if (map.get(key) instanceof APersistentSet)
                {
                    ht.put(key.toString().substring(1), (Object[])toArrayObject((APersistentSet) map.get(key)));
                }
                else
                {
                    ht.put(key.toString().substring(1), map.get(key));
                }
            }
        }
        return ht;
    }

    /**
     * 剔除注释
     * */
    public static String eliminate_comment(String ps)
    {
        String pattern = "(?<=--)[\\s\\S]*?(?=\\n)";
        Pattern p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE|Pattern.MULTILINE);
        Matcher m = p.matcher(ps);
        String one = m.replaceAll("");

        p = Pattern.compile("(?<=/\\*)[\\s\\S]*?(?=\\*/)", Pattern.CASE_INSENSITIVE|Pattern.MULTILINE);
        m = p.matcher(one);
        one = m.replaceAll("");

        p = Pattern.compile("/\\*\\*/", Pattern.CASE_INSENSITIVE|Pattern.MULTILINE);
        m = p.matcher(one);
        one = m.replaceAll("");

        p = Pattern.compile("--", Pattern.CASE_INSENSITIVE|Pattern.MULTILINE);
        m = p.matcher(one);
        one = m.replaceAll("");

        p = Pattern.compile("\n", Pattern.CASE_INSENSITIVE|Pattern.MULTILINE);
        m = p.matcher(one);
        one = m.replaceAll("");

        p = Pattern.compile("\b", Pattern.CASE_INSENSITIVE|Pattern.MULTILINE);
        m = p.matcher(one);
        one = m.replaceAll("");

        return one;
    }
}

