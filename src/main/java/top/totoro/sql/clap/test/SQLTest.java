package top.totoro.sql.clap.test;

import com.google.gson.Gson;
import top.totoro.sql.clap.SQLBatch;
import top.totoro.sql.clap.SQLBean;
import top.totoro.sql.clap.SQLService;
import top.totoro.sql.clap.uitl.Log;

import java.io.IOException;

/**
 * 数据库功能的测试。
 */
public class SQLTest {
    private static final String TAG = "SQLTest";

    public static void main(String[] args) throws IOException {
        String dbName = "db";
        String table = "test";
        /* 创建数据库服务 */
        Service service = new Service(dbName);
        /* 删除test表 */
//        Log.d(TAG, "delete table " + service.dropTable(table));
        /* 创建test表 */
        service.createTable(table);

        /* 测试插入数据 */
        for (int i = 0; i < 10000; i++) {
            service.insert(table, new TestBean(i + ""));
        }
//
//        /* 测试表更新 */
//        // 按主键更新
//        service.updateByKey(table, new TestBean("55", "update(55)"));
//        // 按条件和行为更新
//        long normalStart = new Date().getTime();
//        service.updateByCondition(table,
//                bean -> Integer.parseInt(bean.key) > 0,
//                origin -> {
//                    origin.setName("update by normal");
//                    return origin;
//                });
//        Log.d(TAG, "normal update time = " + (new Date().getTime() - normalStart)+"ms");
//
//        /* 测试查找数据 */
//        // 查找全部
//        Log.d(TAG, "get all size = " + service.selectAll(table).size());
//        // 按条件查找
//        Log.d(TAG, "get key = 10 , size = " + service.selectByCondition(table, bean -> bean.getKey().equals("10")).size());
//        // 按主键查找
//        Log.d(TAG, "get key = 55 , result = " + service.selectByKey(table, "55").getName());
//
//        /* 测试删除数据 */
//        // 根据主键删除
//        Log.d(TAG, "delete by key = 55 , result = " + service.deleteByKey(table, "55"));
//        // 根据条件删除
//        Log.d(TAG, "delete by key > 55 , size = " + service.deleteByCondition(table,
//                bean -> Integer.parseInt(bean.getKey()) > 55).size());
//        // 删除全部
//        Log.d(TAG, "delete all size = " + service.deleteAll(table).size());
//
        // 测试批处理
        SQLBatch<TestBean> batch = new SQLBatch<>(service);
//        /* 测试基本功能 */
//        SQLBatch.BatchTask<Boolean> task = new SQLBatch.BatchTask<>();
//        task.start(() -> service.deleteByKey(table, "45"), SQLBatch.BatchMode.DELETE, 10)
//                .then(respond -> Log.d(TAG, "batch respond = " + respond));
//
//        /* 测试批量插入 */
//        ArrayList<TestBean> list = new ArrayList<>();
//        for (int i = 0; i < 100000; i++) {
//            list.add(new TestBean(i + ""));
//        }
//        batch.insertBatch(table, list, respond -> Log.d(TAG, "insert into test1 result = " + respond));

//        /* 测试批量更新 */
//        batch.updateBatch(table,
//                bean -> Integer.parseInt(bean.key) > 0,
//                origin -> {
//                    origin.setName("update by batch");
//                    return origin;
//                }, null);

//        /* 测试批量查找 */
//        batch.selectBatch(table, bean -> Integer.parseInt(bean.key) >= 0, respond -> Log.d(TAG, "select test1 by batch beans size = " + respond.size()));

//        /* 测试批量删除 */
//        batch.deleteBatch(table, bean -> Integer.parseInt(bean.key) >= 0, null);

        /* 测试不同表批处理的优先级是否互相影响 */
//        String table2 = "test2";
//        service.createTable(table2);
//        batch.insertBatch(table2, list, respond -> Log.d(TAG, "insert into test2 result = " + respond));
//        batch.selectBatch(table2, bean -> Integer.parseInt(bean.key) >= 0, respond -> Log.d(TAG, "select test2 by batch beans size = " + respond.size()));

        Log.d(TAG, "wait system in");
        System.in.read();

    }

    static class Service extends SQLService<TestBean> {
        public Service(String dbName) {
            super(dbName);
        }

        private static final Gson GSON = new Gson();

        @Override
        public String encoderRow(TestBean bean) {
            return GSON.toJson(bean);
        }

        @Override
        public TestBean decoderRow(String row) {
            return GSON.fromJson(row, TestBean.class);
        }
    }

    static class TestBean extends SQLBean {

        String name = "default";

        public TestBean(String key) {
            this.key = key;
        }

        public TestBean(String key, String name) {
            this.key = key;
            this.name = name;
        }

        public void setKey(String key) {
            this.key = key;
        }

        @Override
        public String getKey() {
            return key;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean isSame(Object another) {
            return true;
        }

    }
}
