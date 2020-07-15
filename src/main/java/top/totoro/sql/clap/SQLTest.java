package top.totoro.sql.clap;

import com.google.gson.Gson;

public class SQLTest {
    private static final String TAG = "SQLTest";

    public static void main(String[] args) {
        String table = "test";
        /* 创建数据库服务 */
        Service service = new Service("D:\\common\\local\\sql");
        /* 删除test表 */
        Log.d(TAG,"delete table " + service.dropTable(table));
        /* 创建test表 */
        service.createTable(table);

        /* 测试插入数据 */
        for (int i = 0; i < 100; i++) {
            service.insert(table, new TestBean(i + ""));
        }

        /* 测试表更新 */
        // 按主键更新
        service.updateByKey(table, new TestBean("55", "update(55)"));
        // 按条件和行为更新
        service.updateByCondition(table,
                bean -> bean.getName().equals("update55"),
                origin -> {
                    origin.setName("updateByCondition(55)");
                    return origin;
                });

        /* 测试查找数据 */
        // 查找全部
        Log.d(TAG, "get all size = " + service.selectAll(table).size());
        // 按条件查找
        Log.d(TAG, "get key = 10 , size = " + service.selectByCondition(table, bean -> bean.getKey().equals("10")).size());
        // 按主键查找
        Log.d(TAG, "get key = 55 , result = " + service.selectByKey(table, "55").getName());

        /* 测试删除数据 */
        // 根据主键删除
        Log.d(TAG, "delete by key = 55 , result = " + service.deleteByKey(table, "55"));
        // 根据条件删除
        Log.d(TAG, "delete by key > 55 , size = " + service.deleteByCondition(table,
                bean -> Integer.parseInt(bean.getKey()) > 55).size());
        // 删除全部
        Log.d(TAG, "delete all size = " + service.deleteAll(table).size());

    }

    static class Service extends SQLService<TestBean> {
        public Service(String dbPath) {
            super(dbPath);
        }
        private static final Gson GSON = new Gson();

        @Override
        String encoderRow(TestBean bean) {
            Log.d(TAG, "encoderRow = " + GSON.toJson(bean));
            return GSON.toJson(bean);
        }

        @Override
        TestBean decoderRow(String row) {
//            String[] s = row.split(" ");
//            TestBean bean = new TestBean(s[0]);
//            if (s.length > 1) {
//                bean.setName(s[1]);
//            }
            Log.d(TAG, "decoderRow = " + row);
            return GSON.fromJson(row, TestBean.class);
        }
    }

    static class TestBean extends SQLBean {

        String key;
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
        String getKey() {
            return key;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        boolean isSame(Object another) {
            if (another == this) return true;
            return key.equals(((TestBean) another).getKey());
        }
    }
}
