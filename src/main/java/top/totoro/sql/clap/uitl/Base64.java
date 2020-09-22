package top.totoro.sql.clap.uitl;

/**
 * 数据的加密解密
 * 2020/9/22 取消加密解密，因为会概率性出现乱码
 */
public class Base64 {
    /**
     * 加密
     *
     * @param data 需要解密的原字符串
     * @return 返回解密后的新字符串
     */
    public static String encode(String data) {
//        //把字符串转为字节数组
//        byte[] b = data.getBytes();
//        //遍历
//        for (int i = 0; i < b.length; i++) {
//            b[i] += 1;//在原有的基础上+1
//        }
//        return new String(b);
        return data;
    }

    /**
     * 解密
     *
     * @param data 加密后的字符串
     * @return 返回解密后的新字符串
     */
    public static String decode(String data) {
//        //把字符串转为字节数组
//        byte[] b = data.getBytes();
//        //遍历
//        for (int i = 0; i < b.length; i++) {
//            b[i] -= 1;//在原有的基础上-1
//        }
//        return new String(b);
        return data;
    }
}
