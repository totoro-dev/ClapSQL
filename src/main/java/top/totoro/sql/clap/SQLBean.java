package top.totoro.sql.clap;

import java.io.Serializable;

public abstract class SQLBean implements Serializable {

    /**
     * 不需要使用分表的话可以不实现该方法，
     * 主要是防止当需要使用json格式存储时，
     * bean中没有对应的set方法。
     *
     * @param key 主键
     */
    public abstract void setKey(String key);

    /**
     * 如果有需要用到关键字段的话需要给该方法返回一行数据的关键字段。
     *
     * @return 一行的关键字段
     */
    public abstract String getKey();

    @Override
    public boolean equals(Object another) {
        if (super.equals(another)) return true;
        if (getKey() == null || another == null) return false;
        return isSame(another) &&
                getKey().equals(((SQLTest.TestBean) another).getKey());
    }

    /**
     * 提供判断两个bean是否相同的方法
     *
     * @param another 另一个bean
     * @return 是否相同
     */
    public abstract boolean isSame(Object another);

}
