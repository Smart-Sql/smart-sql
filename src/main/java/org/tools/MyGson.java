package org.tools;

import clojure.lang.PersistentVector;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.util.List;

public class MyGson {
    private static Gson gson = new GsonBuilder()
            .enableComplexMapKeySerialization()
            .setDateFormat("yyyy-MM-dd HH:mm:ss")
            .create();

    public static String groupObjToLine(final PersistentVector vs)
    {
        return gson.toJson(vs);
    }

    public static List<Object> lineToObj(final String line)
    {
        return gson.fromJson(line, new TypeToken<List<Object>>(){}.getType());
    }
}






























