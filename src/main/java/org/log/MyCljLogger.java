package org.log;

import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentVector;
import com.google.common.base.Charsets;
import com.google.common.io.CharSink;
import com.google.common.io.FileWriteMode;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MyCljLogger {

    public static void myWriter(final String txt) {
        try
        {
//            File f = new File(this.getClass().getResource("").getPath());
//            String path = f.getCanonicalPath();
//            System.out.println(path);
//            path = path + "/my_super_con.log";
            String path = "/Users/chenfei/Documents/Java/MyGridGainServer/my_log/my_super_clj_con.log";

            List<String> lst = new ArrayList<>();
            lst.add(txt);
            //lst.add(path_1);
            //CharSink sink = Files.asCharSink(new File("src/main/resources/sample.txt"), Charsets.UTF_8, FileWriteMode.APPEND);
            CharSink sink = Files.asCharSink(new File(path), Charsets.UTF_8, FileWriteMode.APPEND);
            sink.writeLines(lst.stream());
        }
        catch (IOException e)
        {}
    }

    public static void showParams_obj(Object... ps)
    {
        System.out.println(ps);
    }

    public static void showParams(PersistentArrayMap vs)
    {
        System.out.println(vs);
    }

    public static void showParams(PersistentVector vs)
    {
        System.out.println(vs);
    }

}
