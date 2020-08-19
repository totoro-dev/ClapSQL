package top.totoro.sql.clap;

import top.totoro.sql.clap.batch.ThenTask;
import top.totoro.sql.clap.uitl.Base64;
import top.totoro.sql.clap.uitl.IDKit;
import top.totoro.sql.clap.uitl.Log;

import java.io.*;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * 基本的数据库服务，每个不同的需求都可以通过继承该类实现不同的服务。
 * 基础服务是同步方式进行的，如果需要异步批处理的话需要结合{@link SQLBatch}使用。
 * 创建时间 2020/7/8 22:50
 *
 * @author dragon
 * @version 1.0
 */
@SuppressWarnings("ALL")
public abstract class SQLService<Bean extends SQLBean> {
    private static final String TAG = "SQLService";
    // 需要根据具体本地环境设置具体的本地数据库的路径
    private String dbPath
            = System.getProperty("java.io.tmpdir")
            + File.separator + "clap_db"
            + File.separator + getClass().getPackage().getName()
            + "." + getClass().getSimpleName();
    private static final String tableFileSuffix = ".tab";            // 表的文件后缀
    private static final int maxTableFiles = 0x3f;                   // 一个表中允许最多多少个子表，用于对key进行分表
    // f = 16; 1f = 32; 2f = 32; 3f = 64; 4f = 32; 5f = 64
    private static final String ROW_END = " ~end";
    private static final String ROW_SEPARATOR = ROW_END + System.getProperty("line.separator");  // 换行符
    private final SQLCache<Bean> sqlCache;
    private String tableName;

    public SQLService(String dbName) {
        this.dbPath += File.separator + dbName;
        sqlCache = new SQLCache();
        // 通过getGenericSuperclass获取service的类型，包含了
        sqlCache.loadPersistentCache(dbPath, ((ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments()[0]);
        Log.d(this, "db path = " + dbPath);
    }

    /**
     * 需要不同的需求指定将Bean处理成一个可被存储的字符串，
     * 这个字符串必须是可以在获取数据时被解析的。
     *
     * @param bean 需要被编制的实体
     * @return bean可以存储到表中的字符串
     */
    public abstract String encoderRow(Bean bean);

    /**
     * 根据提供的一行数据的字符串，封装成对应的实体。
     *
     * @param line 表中的一行数据的字符串
     * @return 对应数据行的bean
     */
    public abstract Bean decoderRow(String line);

    /**
     * 创建一个表，如果表已经存在则不会重复创建。
     *
     * @param tableName 创建的表名
     * @return 是否创建成功或者是否已经存在。
     */
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
        if (tableFile == null) {
            Log.e(TAG, "getTableFileBeans(tableFile: null) please sure table has created!");
            return beanLines;
        }
        try (FileReader reader = new FileReader(tableFile); BufferedReader bufferedReader = new BufferedReader(reader)) {
            String line, row = "";
            // 一行一行的读取数据
            while ((line = bufferedReader.readLine()) != null) {
                row += line;
                if (line.endsWith(ROW_END)) {
                    beanLines.add(decoderRow(Base64.decode(row.replace(ROW_END, ""))));
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
            Log.e(TAG, "getSubTableFileOrCreate(table: " + table + ", id: " + id + ") failed: parent table not exist, please create table first");
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
            newTableInfo.append(Base64.encode(encoderRow(b)) + ROW_SEPARATOR);
        });
        try (FileWriter fileWriter = new FileWriter(tableFile, false)) {
            fileWriter.write(newTableInfo.toString());
//            Log.d(TAG, "refreshTable tableFile = " + tableFile.getAbsolutePath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 使用同步的方式插入一行数据。
     *
     * @param tableName 插入的表名
     * @param row       插入的数据
     * @return 是否成功插入
     */
    public synchronized boolean insert(String tableName, Bean row) {
        assert row != null;
        File tableFile;
        if (row.getKey() == null) {
            // 不采用分表模式
            tableFile = getSubTableFileOrCreate(tableName, null);
        } else {
            tableFile = getSubTableFileOrCreate(tableName, getKeyId(row.getKey()));
        }
        if (tableFile == null) {
            Log.e(TAG, "insert into " + tableName + " failed," +
                    " please ensure table has created!");
            return false;
        }
        List<Bean> beans = getTableFileBeans(tableFile);
        sqlCache.putToCaching(tableFile.getAbsolutePath(), beans);
        if (!beans.contains(row)) {
            beans.add(row);
            refreshTable(tableFile, beans);
        } else {
            return false;
        }
        return true;
    }

    /**
     * 向确定的表文件中插入数据，用于批处理任务。
     *
     * @param tableFile
     * @param rows
     * @return
     */
    protected synchronized boolean insert(String tableName, File tableFile, List<Bean> rows) {
        if (tableFile == null) {
            Log.e(TAG, "insert into " + tableName + " failed," +
                    " please ensure table has created!");
            return false;
        }
        assert !rows.isEmpty();
        List<Bean> beans = sqlCache.getInCaching(tableFile.getAbsolutePath());
        if (beans == null || beans.isEmpty()) {
            beans = getTableFileBeans(tableFile);
        }
        for (Bean row : rows) {
            if (!beans.contains(row)) {
                beans.add(row);
            }
        }
        refreshTable(tableFile, beans);
        sqlCache.putToCaching(tableFile.getAbsolutePath(), beans);
        return true;
    }

    /**
     * 以主键为查找条件，使用同步的方式查找对应的一行数据。
     *
     * @param tableName 查找的表名
     * @param key       查找的主键
     * @return 一行数据或不存在null
     */
    public Bean selectByKey(String tableName, String key) {
        assert key != null;
        File tableFile = getSubTableFile(tableName, getKeyId(key));
        if (tableFile == null) {
            Log.e(TAG, "select from " + tableName + " by key = " + key + " failed," +
                    " because of table " + tableName + " has not created," +
                    " please ensure table has created!");
            return null;
        }
        Bean caching = sqlCache.getInCaching(tableFile.getAbsolutePath(), key);
        if (caching == null) {
            List<Bean> beans = getTableFileBeans(tableFile);
            for (Bean tableFileBean : beans) {
                if (key.equals(tableFileBean.getKey())) {
                    sqlCache.putToCaching(tableFile.getAbsolutePath(), beans);
                    return tableFileBean;
                }
            }
        }
        return caching;
    }

    /**
     * 使用自定义的查询条件查找数据集合。
     *
     * @param tableName 查找的表名
     * @param condition 自定义的查询条件
     * @return 符合查询条件的数据集，不存在则size为0
     */
    public ArrayList<Bean> selectByCondition(String tableName, Condition<Bean> condition) {
        assert condition != null;
        File[] tableFiles = getAllSubTableFile(tableName);
        ArrayList<Bean> allBeans = new ArrayList<>();
        if (tableFiles == null) {
            Log.e(TAG, "select from " + tableName + " by condition failed," +
                    " because of no target table exist!");
            return allBeans;
        }
        for (File tableFile : tableFiles) {
            List<Bean> caching = sqlCache.getInCaching(tableFile.getAbsolutePath());
            // changed by dragon on 2020/7/23
            // 如果是由于delete后清除缓存导致caching不为空但是数据量为0时，会导致获取不到数据
            // 所以这里需要添加isEmpty的判断，其它方法也要注意这个问题
            if (caching != null && !caching.isEmpty()) {
                for (Bean tableFileBean : caching) {
                    if (condition.accept(tableFileBean)) {
                        allBeans.add(tableFileBean);
                    }
                }
                continue;
            }
            // 需要一个一个子表的去查找
            List<Bean> beans = getTableFileBeans(tableFile);
            boolean hasAccepted = false;
            for (Bean tableFileBean : beans) {
                if (condition.accept(tableFileBean)) {
                    allBeans.add(tableFileBean);
                    hasAccepted = true;
                }
            }
            if (hasAccepted) {
                sqlCache.putToCaching(tableFile.getAbsolutePath(), beans);
            }
        }
        return allBeans;
    }

    /**
     * 使用同步的方式查找整张表的数据集合
     *
     * @param tableName 查找的表名
     * @return 整张表的数据集，或者size为0
     */
    public List<Bean> selectAll(String tableName) {
        File[] tableFiles = getAllSubTableFile(tableName);
        List<Bean> allBeans = new ArrayList<>();
        if (tableFiles == null) {
            Log.e(TAG, "select all from " + tableName + " failed, because of no target table exist!");
            return allBeans;
        }
        for (File tableFile : tableFiles) {
            List<Bean> caching = sqlCache.getInCaching(tableFile.getAbsolutePath());
            if (caching != null && !caching.isEmpty()) {
                allBeans.addAll(caching);
                continue;
            }
            // 获取全部时，不能在缓存中拿了，因为可能缓存中并不包含一个表的所有内容
            List<Bean> beans = getTableFileBeans(tableFile);
            sqlCache.putToCaching(tableFile.getAbsolutePath(), beans);
            allBeans.addAll(beans);
        }
        return allBeans;
    }

    /**
     * 同步方式的不同表之间的嵌套查询，
     * 在上一个表的查询结果中继续匹配当前表中存在的结果并返回。
     * 可以将此次的嵌套查询结果作为下一个表的嵌套查询的基础。
     *
     * @param tableName  当前嵌套的表名
     * @param condition  当前嵌套的表的查询条件
     * @param lastResult 上一个表的查询结果，可以使用其它的查询结果作为输入，不能为空
     * @return 当前嵌套的查询结果
     */
    public List<Bean> selectNest(String tableName, Condition<Bean> condition, List<Bean> lastResult) {
        List<Bean> allBeans = new ArrayList<>();
        List<Bean> nestResult = selectByCondition(tableName, condition);
        // 匹配结果的最大数据量，也就是上一个结果集lastResult和当前表查询结果集nestResult数量的最小值
        // 可以匹配到该数量的结果时结束剩余部分的匹配，避免其它不必要的匹配过程，提高匹配效率。
        int maxResultSize;
        if (nestResult.size() > lastResult.size()) {
            maxResultSize = lastResult.size();
            for (Bean nest : nestResult) {
                for (Bean last : lastResult) {
                    if (nest.equals(last)) {
                        allBeans.add(nest);
                        // 已经达到最大返回结果的数量，不需要继续查找了
                        if (maxResultSize == allBeans.size()) {
                            return allBeans;
                        }
                    }
                }
            }
        } else {
            maxResultSize = nestResult.size();
            for (Bean nest : lastResult) {
                for (Bean last : nestResult) {
                    if (nest.equals(last)) {
                        allBeans.add(nest);
                        // 已经达到最大返回结果的数量，不需要继续查找了
                        if (maxResultSize == allBeans.size()) {
                            return allBeans;
                        }
                    }
                }
            }
        }
        return allBeans;
    }

    /**
     * 确定子表文件时直接更新表，批处理任务可用。
     *
     * @param tableFile
     * @param allBeans
     * @param acceptBeans
     * @return 是否更新成功
     */
    protected boolean update(String tableName, File tableFile, List<Bean> allBeans, List<Bean> acceptBeans) {
        if (tableFile == null) {
            Log.e(TAG, "update " + tableName + " by batch failed," +
                    " because of table " + tableName + " has not created, please ensure table has created!");
            return false;
        }
        refreshTable(tableFile, allBeans);
        List<Bean> caching = sqlCache.getInCaching(tableFile.getAbsolutePath());
        if (caching != null && !caching.isEmpty()) {
            // 需要更新缓存中的这些匹配更新条件的bean
            for (Bean acceptBean : acceptBeans) {
                caching.remove(acceptBean);
            }
            caching.addAll(acceptBeans);
        } else {
            sqlCache.putToCaching(tableFile.getAbsolutePath(), allBeans);
        }
        return true;
    }

    /**
     * 以主键为为更新条件，同步的方式更新新的数据。
     *
     * @param tableName 更新的表名
     * @param update    更新后的数据，需要包含正确的主键
     * @return 是否更新成功
     */
    public boolean updateByKey(String tableName, Bean update) {
        // 根据主键更新时，bean的key必须确保存在
        assert update != null && update.getKey() != null;
        if (update == null || update.getKey() == null) {
            Log.e(TAG, "update " + tableName + " by key failed," +
                    " because of update bean or bean's key must not be null!");
            return false;
        }
        File tableFile = getSubTableFile(tableName, getKeyId(update.getKey()));
        if (tableFile == null) {
            Log.e(TAG, "update " + tableName + " by key = " + update.getKey() + " failed," +
                    " because of table " + tableName + " has not created, please ensure table has created!");
            return false;
        }
        List<Bean> caching = sqlCache.getInCaching(tableFile.getAbsolutePath());
        if (caching != null && !caching.isEmpty()) {
            int index = caching.indexOf(update);
            if (index < 0) {
                // 表中不存在要更新的主键
                Log.e(TAG, "update " + tableName + " by key = " + update.getKey() + " failed," +
                        " because of the table has not this bean " + update);
                return false;
            }
            Bean old = caching.remove(index);
            // 需要更新缓存中的这个bean
            caching.remove(old);
            caching.add(update);
            refreshTable(tableFile, caching);
        } else {
            List<Bean> beans = getTableFileBeans(tableFile);
            int index = beans.indexOf(update);
            if (index < 0) {
                // 表中不存在要更新的主键
                Log.e(TAG, "update " + tableName + " by key = " + update.getKey() + " failed," +
                        " because of the table has not this bean " + update);
                return false;
            }
            Bean old = beans.remove(index);
            beans.add(update);
            refreshTable(tableFile, beans);
            sqlCache.putToCaching(tableFile.getAbsolutePath(), beans);
        }
        return true;
    }

    /**
     * 自定义更新条件和对匹配条件的数据进行的操作。
     *
     * @param tableName 更新的表名
     * @param condition 自定义条件
     * @param operation 更新操作
     * @return 是否全部更新成功
     */
    public boolean updateByCondition(String tableName, Condition<Bean> condition, Operation<Bean> operation) {
        assert condition != null && operation != null;
        File[] tableFiles = getAllSubTableFile(tableName);
        if (tableFiles == null) {
            Log.e(TAG, "update " + tableName + " by condition failed," +
                    " because of no target table exist!");
            return false;
        }
        for (File tableFile : tableFiles) {
            List<Bean> allAcceptBeans = new ArrayList<>();
            List<Bean> beans = sqlCache.getInCaching(tableFile.getAbsolutePath());
            if (beans == null || beans.isEmpty()) {
                // 缓存中没有这个子表的数据，需要去子表中查找
                beans = getTableFileBeans(tableFile);
            }
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
                    sqlCache.putToCaching(tableFile.getAbsolutePath(), beans);
                }
            }
            refreshTable(tableFile, beans);
        }
        return true;
    }

    protected boolean delete(File tableFile, List<Bean> subTableBeans, List<Bean> acceptBeans) {
        if (tableFile == null) {
            Log.e(TAG, "delete " + tableName + " by batch failed," +
                    " because of table " + tableName + " has not created," +
                    " please ensure table has created!");
            return false;
        }
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

    /**
     * 以主键为条件删除指定的一行数据。
     *
     * @param tableName 删除数据的表名
     * @param key       数据的主键值
     * @return 是否删除成功
     */
    public boolean deleteByKey(String tableName, String key) {
        assert key != null;
        File tableFile = getSubTableFile(tableName, getKeyId(key));
        if (tableFile == null) {
            Log.e(TAG, "delete " + tableName + " by key = " + key + " failed," +
                    " because of table " + tableName + " has not created," +
                    " please ensure table has created!");
            return false;
        }
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

    /**
     * 自定义删除条件，对匹配的数据行执行删除操作。
     *
     * @param tableName 要删除数据的表名
     * @param condition 自定义删除条件
     * @return 正确删除的数据集合
     */
    public List<Bean> deleteByCondition(String tableName, Condition<Bean> condition) {
        assert condition != null;
        File[] tableFiles = getAllSubTableFile(tableName);
        List<Bean> allAcceptBeans = new ArrayList<>();
        if (tableFiles == null) {
            Log.e(TAG, "delete " + tableName + " by condition failed," +
                    " because of table " + tableName + " has not created," +
                    " please ensure table has created!");
            return allAcceptBeans;
        }
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

    /**
     * 删除指定的整张表的数据。
     *
     * @param tableName 删除的表名
     * @return 删除了的数据集合
     */
    public List<Bean> deleteAll(String tableName) {
        File[] tableFiles = getAllSubTableFile(tableName);
        List<Bean> allAcceptBeans = new ArrayList<>();
        if (tableFiles == null) {
            Log.e(TAG, "delete all from " + tableName + " failed," +
                    " because of table " + tableName + " has not created," +
                    " please ensure table has created!");
            return allAcceptBeans;
        }
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

    /**
     * 删除掉整个表文件。
     *
     * @param tableName 要删除的表名
     * @return 表目录是否删除，如果目录中存在非数据表的文件则不会删除目录，但表文件会正确删除；
     * 如果目录不存在其他类型文件，则整个目录删除，返回true。
     */
    public boolean dropTable(String tableName) {
        System.gc();
        File table = new File(dbPath + File.separator + tableName + File.separator);
        // 删除所有的子表
        File[] tableFiles = getAllSubTableFile(tableName);
        if (tableFiles == null) {
            Log.e(TAG, "drop table " + tableName + " failed," +
                    " because of table " + tableName + " has not created," +
                    " please ensure table has created!");
            return false;
        }
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
        return true;
    }

    // id最多是32位，需要降到16位
    // 再和最大文件数进行与运算即可
    private static long hash(long id) {
        return (id ^ (id >>> 16)) & maxTableFiles;
    }

    /**
     * 获取主键的唯一id
     *
     * @param key 主键的值
     * @return 主键的唯一id
     */
    public Long getKeyId(String key) {
        // 防止IDKit报空指针
        if (key == null) return 0L;
        return IDKit.getUniqueID(key);
    }

    static int requestIndex(int size, long id) {
//        return size & (int) (id & Integer.MAX_VALUE);
        return size & (int) id;
    }

    /**
     * 获取一个数据库服务是否具有对当前数据库执行操作的权限
     *
     * @param compare 另一个数据库服务
     * @return true，允许操作该数据库；否则不允许
     */
    public boolean getAuthority(SQLService compare) {
        String curPackage = getClass().getPackage().getName();
        return compare.getClass().getPackage().getName().contains(curPackage);
    }

    public interface Condition<Bean extends SQLBean> {
        // 条件满足
        boolean accept(Bean bean);
    }

    public interface Operation<Bean extends SQLBean> {
        Bean operate(Bean origin);
    }
}
