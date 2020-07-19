# ClapSQL
## 特性
 缩写 | 全称 | 释义
 :---: | :---: | ---
 C | Client | 客户端，无需考虑服务不可用问题
 L | Light | 轻量级，不依赖底层系统，无需安装任何依赖，有Java的地方就能跑
 A | Almighty | 全能型，除了基本的CRUD，还具备分表、异步、批处理等能力
 P | Performance | 性能高，内部使用缓存和批处理机制减少IO操作，哈希分表减少单个表文件处理的数据量，快速定位数据
 其他 | 无 | 数据可加密，管理访问权限，代码逻辑替代SQL语句的编写
 ## 意图
 客户端的开发其实或多或少会使用到数据库，比如应用埋点的数据记录、用户数据的记录等等。
  - 客户端是基本无需考虑高并发带来的数据库压力的，所以大型的数据库根本就用不上；
  - 单个用户产生的数据量是可预见的，所以一个小而精悍的数据库就特别合适；
  - 少量的数据当然可以自己用文件解决，但是每次写IO流程的开发效率可想而知；
  - 关系型数据库基本都是需要编写SQL语句的，使用ClapSQL无需编写任何的SQL语句。
## 用法
1. 需要自定义一个数据实体TestBean，并继承自SQLBean

~~~java
public class TestBean extends SQLBean {

    String key;
    String name = "default";

    public TestBean(String key) {
        this.key = key;
    }

    public TestBean(String key, String name) {
        this.key = key;
        this.name = name;
    }

    @Override
    public void setKey(String key) {
        this.key = key;
    }

    @Override
    String getKey() {
        return key;
    }

    public void setName(String name) {
        this.name = name;
    }
    
    public String getName(){
        return name;
    }

    @Override
    boolean isSame(Object another) {
        return true;
    }

}
~~~

2. 创建使用数据实体TestBean的SQL服务

~~~java
public class Service extends SQLService<TestBean> {
    public Service(String dbPath) {
        super(dbPath);
    }

    private static final Gson GSON = new Gson();

    @Override
    String encoderRow(TestBean bean) {
        /* 方式1 ：将bean封装成json字符串 */
        return GSON.toJson(bean);
        /* 方式2 ：自定义封装，可被解析成bean对象即可
        return bean.getKey();
        */
    }

    @Override
    TestBean decoderRow(String row) {
        /* 方式1 ：获取到的数据表中一行json字符串，需要解析成对应的bean对象 */
        return GSON.fromJson(row, TestBean.class);
        /* 方式2 ：将字符串解析成对应对象
        return new TestBean(row);
        */
    }
}
~~~

3. 使用Service实现简单的数据库操作

~~~java
public class Test{
    public static void main(String[] args) throws IOException {
        String dbName = "db";
        String table = "test";
        /* 创建数据库服务 */
        Service service = new Service(dbName);
        /* 删除test表 */
        service.dropTable(table);
        /* 创建test表 */
        service.createTable(table);

        /* 测试插入数据 */
        for (int i = 0; i < 10000; i++) {
            service.insert(table, new TestBean(i + ""));
        }

        /* 测试表更新 */
        // 按主键更新 : update test set name = 'update(50)' where key = 50;
        service.updateByKey(table, new TestBean("50", "update(50)"));
        // 按条件和操作更新 : update test set name = 'update' where key > 0;
        long normalStart = new Date().getTime();
        service.updateByCondition(table,
                /* 相当于SQL语句：‘where key > 0’ */
                bean -> Integer.parseInt(bean.key) > 0,
                /* 相当于SQL语句：‘set name = 'update'’ */
                origin -> {
                    origin.setName("update");
                    return origin;
                });

        /* 测试查找数据 */
        // 查找全部 : select * from test;
        System.out.println("select all size = "+service.selectAll(table).size());
        // 按主键查找 : select name from test where key = 50;
        System.out.println("select by key = 50 , result = " + service.selectByKey(table, "50").getName());
        // 按条件查找 : select * from test where key = 10;
        System.out.println("select by key = 10 , size = " + 
                service.selectByCondition(table, bean -> bean.getKey().equals("10")).size());

        /* 测试删除数据 */
        // 根据主键删除 : delete test where key = 50;
        System.out.println("delete by key = 50 , result = " + service.deleteByKey(table, "50"));
        // 根据条件删除 : delete test where key > 50;
        System.out.println("delete by key > 50 , size = " + service.deleteByCondition(table,
                bean -> Integer.parseInt(bean.getKey()) > 50).size());
        // 删除全部 : delete test;
        System.out.println("delete all size = " + service.deleteAll(table).size());
    }
}
~~~