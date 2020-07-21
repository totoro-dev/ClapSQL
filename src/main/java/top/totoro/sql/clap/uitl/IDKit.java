package top.totoro.sql.clap.uitl;

import java.util.Date;

/**
 * id生成工具。
 * 为了防止生成的id由于第一位是0被忽略，所以在id头增加了一个标志位，实际有效位只有18位
 * 创建时间 2020/6/26 14:37
 *
 * @author dragon
 * @version 1.0
 */
public class IDKit {
    /**
     * 根据key字符串构建一个唯一id，标志位为1。
     *
     * @param key 关键字段
     * @return 唯一id
     */
    public static Long getUniqueID(String key) {
        // 为了防止超过Long的最大值{9223372036854775807},设置了18位。
        // 但如果不足18位前面的0不会被补足，也就会出现小于18位的情况，但最多18位
        return Long.parseLong("1" + getUnicode(key, 18));
    }

    /**
     * 构建一个包含时间信息的唯一id，标志位为2。
     * 前13位为日期的time，接着是字符串unicode值的后5位。
     *
     * @param key  关键字段
     * @param time 时间
     * @return 唯一id
     */
    public static Long getUniqueIDWithTime(String key, Date time) {
        String uniqueID = "2" +
                time.getTime() +
                getUnicode(key, 5);
        return Long.parseLong(uniqueID);
    }

    /**
     * 创建一个带前缀的唯一id，标志位为3。
     * 前缀占4位，是字符串unicode值的后14位。
     *
     * @param key    关键字段
     * @param prefix 前缀
     * @return 唯一id
     */
    public static Long getUniqueIDWithPrefix(String key, int prefix) {
        StringBuilder uniqueID = new StringBuilder("3");
        if (prefix < 1000) {
            int insertLength = 4 - String.valueOf(prefix).length();
            for (int i = 0; i < insertLength; i++) { /* 补全长度 */
                uniqueID.insert(1, 0);
            }
        }
        uniqueID.append(prefix);
        uniqueID.append(getUnicode(key, 14));
        return Long.parseLong(uniqueID.toString());
    }

    /**
     * 创建一个带后缀的唯一id，标志位为4。
     * 后缀占4位，是字符串unicode值的后14位。
     *
     * @param key    关键字段
     * @param suffix 后缀
     * @return 唯一id
     */
    public static Long getUniqueIDWithSuffix(String key, int suffix) {
        StringBuilder uniqueID = new StringBuilder("4");
        uniqueID.append(getUnicode(key, 14));
        if (suffix < 1000) {
            int insertLength = 4 - String.valueOf(suffix).length();
            for (int i = 0; i < insertLength; i++) { /* 补全长度 */
                uniqueID.insert(15, 0);
            }
        }
        uniqueID.append(suffix);
        return Long.parseLong(uniqueID.toString());
    }

    private static String getUnicode(String key, int length) {
        StringBuilder unicode = new StringBuilder();
        for (int i = key.length() - 1; i >= 0 && unicode.length() < length; i--) {
            // 每个字符转化为Unicode编码，进行组合
            //noinspection UnnecessaryCallToStringValueOf
            unicode.insert(0, Integer.toString(key.charAt(i)));
        }
        if (unicode.length() < length) { /* 补全长度 */
            int insertLength = length - unicode.length();
            for (int i = 0; i < insertLength; i++) {
                unicode.insert(0, 0);
            }
        }
//        Log.d(IDKit.class.getSimpleName(), "unicode = " + unicode.substring(unicode.length() - length));
        return unicode.substring(unicode.length() - length);
    }

}
