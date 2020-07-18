package top.totoro.sql.clap;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
/**
 * 创建时间 2020/7/8 22:50
 *
 * @author dragon
 * @version 1.0
 */
@SuppressWarnings("ALL")
public abstract class SQLService<Bean extends SQLBean> {
    private static final String TAG = "SQLService";
    // 需要根据具体本地环境设置具体的本地数据库的路径
    private String dbPath = "D:\\训练样本\\resources\\train";    // 数据库路径，以最后一个目录作为数据库名
    public static final String tableFileSuffix = ".tab";            // 表的文件后缀
    public static final int maxTableFiles = 0xff;                   // 一个表中允许最多多少个子表，用于对key进行分表
    private static final String ROW_END = " ~end";
    public static final String ROW_SEPARATOR = ROW_END + System.getProperty("line.separator");  // 换行符
    private final SQLCache<Bean> sqlCache;
    private String tableName;

    public SQLService(String dbPath) {
        this.dbPath = dbPath;
        sqlCache = new SQLCache();
    }

    /**
     * 需要不同的需求制定将Bean处理成一个可被存储的字符串，
     * 这个字符串必须是可以在获取数据时被解析的。
     *
     * @param bean 需要被编制的实体
     * @return bean可以存储到表中的字符串
     */
    abstract String encoderRow(Bean bean);

    /**
     * 根据提供的一行数据的字符串，封装成对应的实体。
     *
     * @param line 表中的一行数据的字符串
     * @return 对应数据行的bean
     */
    abstract Bean decoderRow(String line);

    /**
     * 设置该服务所对应的数据库的本地路径
     *
     * @param dbPath 本地路径
     */
    public void setDBPath(String dbPath) {
        dbPath = dbPath;
    }

    public boolean createTable(String tableName) {
        assert tableName != null;
        this.tableName = tableName;
        try {
            // 确定表是否存在
            String tableRootPath = dbPath + File.separator + tableName;
            File tableRootFile = new File(tableRootPath);
            if (!tableRootFile.exists() || !tableRootFile.isDirectory()) {
                // 创建表目录
                tableRootFile.mkdirs();
                Log.d(TAG, "createTable mkdirs() path = " + tableRootPath);
            }
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    /**
     * 获取一个表文件中的所有数据行。
     *
     * @param tableFile 表文件
     * @return 所有的行
     */
    protected LinkedList<Bean> getTableFileBeans(File tableFile) {
        LinkedList<Bean> beanLines = new LinkedList<>();
        try (FileReader reader = new FileReader(tableFile); BufferedReader bufferedReader = new BufferedReader(reader)) {
            String line, row = "";
            // 一行一行的读取数据
            while ((line = bufferedReader.readLine()) != null) {
                row += line;
                if (line.endsWith(ROW_END)) {
                    beanLines.add(decoderRow(row.replace(ROW_END, "")));
                    row = "";
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return beanLines;
    }

    /**
     * 获取关键字段唯一id为id对应的是哪个子表文件的数据，
     * 如果id为null，则说明不使用分表的规则获取表文件，默认表文件名为'0.tab'
     * 只有数据实体bean真正实现了{@link SQLBean#getKey()}，
     * 才有办法对表拆分出子表，这样id才会有效。
     * 这个id必须通过{@link IDKit}获取
     *
     * @param table 表名
     * @param id    关键字段某一行数据的key的唯一id
     * @return 表中唯一id值为id的子表的表文件，不存在该子表则创建它
     */
    protected File getSubTableFileOrCreate(String table, Long id) {
        // 确定表是否存在
        String tableRootPath = dbPath + File.separator + table;
        File tableRootFile = new File(tableRootPath);
        if (!tableRootFile.exists() || !tableRootFile.isDirectory()) {
            Log.d(TAG, "getSubTableFileOrCreate(table: " + table + "id: " + id + ") failed: parent table not exist, please create table first");
            return null;
        }

        // 确定bean对应的key的子表文件名
        long fileName;
        if (id == null) {
            fileName = 0;
        } else {
            fileName = hash(id);
        }

        // 子表的表路径
        String tableFilePath = tableRootPath + File.separator + fileName + tableFileSuffix;
        File tableFile = new File(tableFilePath);
        if (!tableFile.exists()) {
            try {
                // 创建表路径下的子表文件
                tableFile.createNewFile();
                Log.d(TAG, "createNewFile() path = " + tableFilePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return tableFile;
    }

    /**
     * 获取存在的子表，子表名由关键字段的唯一id决定
     * 如果id为null，则说明不使用分表的规则获取表文件，默认表文件名为'0.tab'
     *
     * @param table 表名
     * @param id    关键字段的唯一id
     * @return 存在的子表文件
     */
    protected File getSubTableFile(String table, Long id) {
        // 确定表是否存在
        String tableRootPath = dbPath + File.separator + table;
        File tableRootFile = new File(tableRootPath);
        if (!tableRootFile.exists() || !tableRootFile.isDirectory()) {
            Log.e(TAG, "getSubTableFile(table: " + table + "id: " + id + ") failed: parent table not exist, please create table first.");
            return null;
        }

        // 确定bean对应的key的子表文件名
        long fileName;
        if (id == null) {
            fileName = 0;
        } else {
            fileName = hash(id);
        }

        // 子表的表路径
        String tableFilePath = tableRootPath + File.separator + fileName + tableFileSuffix;
        File tableFile = new File(tableFilePath);
        if (!tableFile.exists()) {
            Log.e(TAG, "getSubTableFile(table: " + table + "id: " + id + ") failed: sub table file " + fileName + tableFileSuffix + " not exist.");
            return null;
        }
        return tableFile;
    }

    /**
     * 获取存在的子表，子表名由关键字段的唯一id决定
     * 如果id为null，则说明不使用分表的规则获取表文件，默认表文件名为'0.tab'
     *
     * @param table 表名
     * @param id    关键字段的唯一id
     * @return 存在的子表文件
     */
    protected File[] getAllSubTableFile(String table) {
        // 确定表是否存在
        String tableRootPath = dbPath + File.separator + table;
        File tableRootFile = new File(tableRootPath);
        if (!tableRootFile.exists() || !tableRootFile.isDirectory()) {
            Log.e(TAG, "getAllSubTableFile(table: " + table + ") failed: parent table not exist, please create table first.");
            return null;
        }

        File[] subTableFiles = tableRootFile.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getAbsolutePath().endsWith(tableFileSuffix);
            }
        });
        if (subTableFiles == null) {
            Log.e(TAG, "getAllSubTableFile(table: " + table + ") failed: sub table file not exist.");
            return null;
        }
        return subTableFiles;
    }

    /**
     * 将新的内容更新到表文件
     *
     * @param tableFile    表文件
     * @param beansInTable 表的最新内容
     */
    protected void refreshTable(File tableFile, List<Bean> beansInTable) {
        StringBuilder newTableInfo = new StringBuilder();
        beansInTable.forEach(b -> {
            if (b == null) return;
            newTableInfo.append(encoderRow(b) + ROW_SEPARATOR);
        });
        try (FileWriter fileWriter = new FileWriter(tableFile, false)) {
            fileWriter.write(newTableInfo.toString());
//            Log.d(TAG, "refreshTable tableFile = " + tableFile.getAbsolutePath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized boolean insert(String tableName, Bean row) {
        assert row != null;
        File tableFile;
        if (row.getKey() == null) {
            // 不采用分表模式
            tableFile = getSubTableFileOrCreate(tableName, null);
        } else {
            tableFile = getSubTableFileOrCreate(tableName, getKeyId(row.getKey()));
        }
        assert tableFile != null;
        List<Bean> beans = getTableFileBeans(tableFile);
        if (!beans.contains(row)) {
            beans.add(row);
            refreshTable(tableFile, beans);
        }
        sqlCache.putToCaching(tableFile.getAbsolutePath(), row);
        return true;
    }

    /**
     * 向确定的表文件中插入数据，用于批处理任务。
     *
     * @param tableFile
     * @param rows
     * @return
     */
    protected synchronized boolean insert(File tableFile, List<Bean> rows) {
        assert !rows.isEmpty();
        assert tableFile != null;
        List<Bean> beans = getTableFileBeans(tableFile);
        for (Bean row : rows) {
            if (!beans.contains(row)) {
                beans.add(row);
            }
        }
        refreshTable(tableFile, beans);
        sqlCache.putToCaching(tableFile.getAbsolutePath(), rows);
        return true;
    }

    public Bean selectByKey(String tableName, String key) {
        assert key != null;
        File tableFile = getSubTableFile(tableName, getKeyId(key));
        Bean caching = sqlCache.getInCaching(tableFile.getAbsolutePath(), key);
        if (caching == null) {
            List<Bean> beans = getTableFileBeans(tableFile);
            for (Bean tableFileBean : beans) {
                if (key.equals(tableFileBean.getKey())) {
                    sqlCache.putToCaching(tableFile.getAbsolutePath(), tableFileBean);
                    return tableFileBean;
                }
            }
        }
        return caching;
    }

    public ArrayList<Bean> selectByCondition(String tableName, Condition<Bean> condition) {
        assert condition != null;
        File[] tableFiles = getAllSubTableFile(tableName);
        ArrayList<Bean> allBeans = new ArrayList<>();
        for (File tableFile : tableFiles) {
            // 需要一个一个子表的去查找
            List<Bean> beans = getTableFileBeans(tableFile);
            for (Bean tableFileBean : beans) {
                if (condition.accept(tableFileBean)) {
                    sqlCache.putToCaching(tableFile.getAbsolutePath(), tableFileBean);
                    allBeans.add(tableFileBean);
                }
            }
        }
        return allBeans;
    }

    public List<Bean> selectAll(String tableName) {
        File[] tableFiles = getAllSubTableFile(tableName);
        List<Bean> allBeans = new ArrayList<>();
        for (File tableFile : tableFiles) {
            // 获取全部时，不能在缓存中拿了，因为可能缓存中并不包含一个表的所有内容
            List<Bean> beans = getTableFileBeans(tableFile);
            sqlCache.putToCaching(tableFile.getAbsolutePath(), beans);
            allBeans.addAll(beans);
        }
        return allBeans;
    }

    /**
     * 确定子表文件时直接更新表，批处理任务可用。
     *
     * @param tableFile
     * @param allBeans
     * @param acceptBeans
     * @return
     */
    protected boolean update(File tableFile, List<Bean> allBeans, List<Bean> acceptBeans) {
        assert tableFile != null;
        refreshTable(tableFile, allBeans);
        List<Bean> caching = sqlCache.getInCaching(tableFile.getAbsolutePath());
        if (caching != null) {
            // 需要更新缓存中的这些匹配更新条件的bean
            for (Bean acceptBean : acceptBeans) {
                caching.remove(acceptBean);
            }
            caching.addAll(acceptBeans);
        } else {
            sqlCache.putToCaching(tableFile.getAbsolutePath(), acceptBeans);
        }
        return true;
    }

    public boolean updateByKey(String tableName, Bean update) {
        // 根据主键更新时，bean的key必须确保存在
        assert update != null && update.getKey() != null;
        File tableFile = getSubTableFile(tableName, getKeyId(update.getKey()));
        assert tableFile != null;
        List<Bean> beans = getTableFileBeans(tableFile);
        int index = beans.indexOf(update);
        if (index < 0) {
            // 表中不存在要更新的主键
            Log.e(TAG, "The table has not this bean " + update);
            return false;
        }
        Bean old = beans.remove(index);
        beans.add(update);
        refreshTable(tableFile, beans);
        List<Bean> caching = sqlCache.getInCaching(tableFile.getAbsolutePath());
        if (caching != null) {
            // 需要更新缓存中的这个bean
            caching.remove(old);
            caching.add(update);
        } else {
            sqlCache.putToCaching(tableFile.getAbsolutePath(), update);
        }
        return true;
    }

    public boolean updateByCondition(String tableName, Condition<Bean> condition, Operation<Bean> operation) {
        assert condition != null && operation != null;
        File[] tableFiles = getAllSubTableFile(tableName);
        for (File tableFile : tableFiles) {
            List<Bean> allAcceptBeans = new ArrayList<>();
            // 需要一个一个子表的去查找
            List<Bean> beans = getTableFileBeans(tableFile);
            for (Bean tableFileBean : beans) {
                if (condition.accept(tableFileBean)) {
                    allAcceptBeans.add(tableFileBean);
                }
            }
            // 查找的这张表没有匹配的项，查找下一张子表
            if (allAcceptBeans.isEmpty()) continue;
            for (Bean acceptBean : allAcceptBeans) {
                beans.remove(acceptBean);
                beans.add(operation.operate(acceptBean));
                List<Bean> caching = sqlCache.getInCaching(tableFile.getAbsolutePath());
                if (caching != null) {
                    int index = caching.indexOf(acceptBean);
                    if (index < 0) continue;
                    // 需要更新缓存中的这个bean
                    caching.remove(index);
                    caching.add(acceptBean);
                } else {
                    sqlCache.putToCaching(tableFile.getAbsolutePath(), acceptBean);
                }
            }
            refreshTable(tableFile, beans);
        }
        return true;
    }

    protected boolean delete(File tableFile, List<Bean> subTableBeans, List<Bean> acceptBeans) {
        refreshTable(tableFile, subTableBeans);
        List<Bean> caching = sqlCache.getInCaching(tableFile.getAbsolutePath());
        if (caching != null) {
            // 需要删除缓存中的这些匹配删除条件的bean
            for (Bean acceptBean : acceptBeans) {
                caching.remove(acceptBean);
            }
        }
        return true;
    }

    public boolean deleteByKey(String tableName, String key) {
        assert key != null;
        File tableFile = getSubTableFile(tableName, getKeyId(key));
        assert tableFile != null;
        List<Bean> beans = getTableFileBeans(tableFile);
        Bean deleteBean = null; // 查找需要删除掉的项
        for (int i = 0; i < beans.size(); i++) {
            deleteBean = beans.get(i);
            if (key.equals(deleteBean.getKey())) {
                break;
            }
        }
        beans.remove(deleteBean);

        refreshTable(tableFile, beans);
        // 刷新缓存
        List<Bean> caching = sqlCache.getInCaching(tableFile.getAbsolutePath());
        if (caching != null) {
            // 需要删除缓存中的这个bean
            caching.remove(deleteBean);
        }
        return true;
    }

    public List<Bean> deleteByCondition(String tableName, Condition<Bean> condition) {
        assert condition != null;
        File[] tableFiles = getAllSubTableFile(tableName);
        List<Bean> allAcceptBeans = new ArrayList<>();
        for (File tableFile : tableFiles) {
            // tableFile子表中存在的所有匹配的bean
            List<Bean> acceptBeans = new ArrayList<>();
            // 需要一个一个子表的去查找
            List<Bean> beans = getTableFileBeans(tableFile);
            for (Bean tableFileBean : beans) {
                if (condition.accept(tableFileBean)) {
                    acceptBeans.add(tableFileBean);
                    beans.remove(tableFileBean);
                }
            }
            if (acceptBeans.isEmpty()) continue;
            refreshTable(tableFile, beans);
            allAcceptBeans.addAll(acceptBeans);
            List<Bean> caching = sqlCache.getInCaching(tableFile.getAbsolutePath());
            if (caching != null) {
                for (Bean acceptBean : acceptBeans) {
                    // 需要删除缓存中的这个bean
                    caching.remove(acceptBean);
                }
            }
        }
        return allAcceptBeans;
    }

    public List<Bean> deleteAll(String tableName) {
        File[] tableFiles = getAllSubTableFile(tableName);
        List<Bean> allAcceptBeans = new ArrayList<>();
        for (File tableFile : tableFiles) {
            // tableFile子表中存在的所有匹配的bean
            List<Bean> acceptBeans = new ArrayList<>();
            // 需要一个一个子表的去查找
            List<Bean> beans = getTableFileBeans(tableFile);
            for (Bean tableFileBean : beans) {
                acceptBeans.add(tableFileBean);
                beans.remove(tableFileBean);
            }
            if (acceptBeans.isEmpty()) continue;
            refreshTable(tableFile, beans);
            allAcceptBeans.addAll(acceptBeans);
            List<Bean> caching = sqlCache.getInCaching(tableFile.getAbsolutePath());
            if (caching != null) {
                for (Bean acceptBean : acceptBeans) {
                    // 需要删除缓存中的这个bean
                    caching.remove(acceptBean);
                }
            }
        }
        return allAcceptBeans;
    }

    public boolean dropTable(String tableName) {
        System.gc();
        File table = new File(dbPath + File.separator + tableName + File.separator);
        // 删除所有的子表
        File[] tableFiles = getAllSubTableFile(tableName);
        if (tableFiles != null) {
            for (File tableFile : tableFiles) {
                tableFile.delete();
            }
        }
        // 如果目录中存在不是表的文件的话，目录不会被删除
        if (table.exists() && table.isDirectory()) {
            Log.d(TAG, "drop table name =" + table.getAbsolutePath());
            return table.delete();
        }
        return false;
    }

    // id最多是32位，需要降到16位
    // 再和最大文件数进行与运算即可
    private static long hash(long id) {
        return (id ^ (id >>> 16)) & maxTableFiles;
    }

    public Long getKeyId(String key) {
        return IDKit.getUniqueID(key);
    }

    static int requestIndex(int size, long id) {
//        return size & (int) (id & Integer.MAX_VALUE);
        return size & (int) id;
    }

    interface Condition<Bean extends SQLBean> {
        // 条件满足
        boolean accept(Bean bean);
    }

    interface Operation<Bean extends SQLBean> {
        Bean operate(Bean origin);
    }
}
