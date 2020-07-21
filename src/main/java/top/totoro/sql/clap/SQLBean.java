package top.totoro.sql.clap;

import java.io.Serializable;

/**
 * 每一行数据的实体类，每个数据库服务都需要继承该类并定义自己的数据实体。
 *
 * @author dragon
 * @version 1.0
 */
public abstract class SQLBean implements Serializable {

    /**
     * 每个表的主键字段，除了区分一行数据，还被用来做分表处理。
     */
    public String key;

    /**
     * 不需要使用分表的话可以不实现该方法，
     * 主要是防止当需要使用json格式存储时，
     * bean中没有对应的set方法。
     *
     * @param key 主键
     */
    public void setKey(String key) {
        this.key = key;
    }

    /**
     * 如果有需要用到关键字段的话需要给该方法返回一行数据的关键字段。
     * 返回null说明该表不使用分表模式，所有的数据只存储到一个表文件中。
     *
     * @return 一行数据的主键字段值
     */
    public String getKey() {
        return key;
    }

    @Override
    public boolean equals(Object another) {
        if (super.equals(another)) return true;
        if (getKey() == null || another == null) return false;
        return isSame(another) &&
                getKey().equals(((SQLBean) another).getKey());
    }

    /**
     * 提供判断两个bean是否相同的方法，
     * 处理主键以外，如果某些字段对区分一行数据是否相同产生影响时，需要加到判断中去。
     * 没有的话必须返回true，因为主键的判断已经有默认实现了。
     *
     * @param another 另一行数据
     * @return 是否是同一行数据
     */
    public abstract boolean isSame(Object another);

}
