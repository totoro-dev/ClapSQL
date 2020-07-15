package top.totoro.sql.clap;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 日志打印。
 * 创建时间 2020/6/26 21:40
 *
 * @author dragon
 * @version 1.0
 */
public class Log {
    private static boolean debug = true;
    private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:SS");

    public static void i(Object tag, String msg) {
        if (!debug) return;
        print("INFO ", tag, msg);
    }

    public static void d(Object tag, String msg) {
        if (!debug) return;
        print("DEBUG", tag, msg);
    }

    public static void w(Object tag, String msg) {
        if (!debug) return;
        print("WARN ", tag, msg);
    }

    public static void e(Object tag, String msg) {
        if (!debug) return;
        print("ERROR", tag, msg);
    }

    public static void l() {
        if (!debug) return;
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
    }

    private static void print(String level, Object tagObj, String msg) {
        String tag = "DEFAULT-TAG";
        if (tagObj instanceof String) {
            tag = (String) tagObj;
        } else if (!(tagObj instanceof Class)) {
            tag = tagObj.getClass().getSimpleName();
        }
        System.out.printf("%s\t%s ----- %s : %s\n", format.format(new Date()), level, tag, msg);
    }

    public static void debug(boolean debug) {
        Log.debug = debug;
    }
}
