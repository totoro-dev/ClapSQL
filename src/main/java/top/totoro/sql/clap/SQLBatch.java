package top.totoro.sql.clap;

import top.totoro.sql.clap.batch.BatchMode;
import top.totoro.sql.clap.batch.BatchTask;
import top.totoro.sql.clap.batch.ThenTask;
import top.totoro.sql.clap.uitl.Log;

import java.io.File;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * 数据库的批处理能力，根据自己项目的需要进行使用，默认是不会使用批处理执行数据库操作的。
 * 创建时间 2020/7/15
 *
 * @author dragon
 * @version 1.0
 */
@SuppressWarnings("ALL")
public class SQLBatch<Bean extends SQLBean> {

    private static final String TAG = "SQLBatch";
    private final SQLService<Bean> sqlService;

    /**
     * 决定这个批处理对象为那个数据库服务。
     *
     * @param sqlService 数据库服务
     */
    public SQLBatch(SQLService<Bean> sqlService) {
        this.sqlService = sqlService;
    }

    // 使用机器的处理器数量创建计划执行的的服务
    public static final ScheduledExecutorService SCHEDULED_EXECUTOR
            = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
    // 存储当前存在的所有批处理对象优先级集合，执行批处理任务时需要根据优先级执行
    /* changed by dragon 2020/07/18 增加一级map，用来区分不同表的批处理任务，使得各个表之间的任务执行优先级不受影响 */
    public static final Map<String, HashMap<BatchMode, LinkedList<BatchTask<? extends Serializable>>>> BATCH_PRIORITY_MAP
            = new ConcurrentHashMap<>();
    // 当前可再利用的批处理空对象
    protected static final Map<BatchMode, List<BatchTask<? extends Serializable>>> BATCH_AVAILABLE_MAP
            = new ConcurrentHashMap<>();

    private int i = 0;

    private synchronized BatchTask<?> obtain(BatchMode mode, Class<?> respondType) {
        List<BatchTask<?>> batchTaskList = BATCH_AVAILABLE_MAP.computeIfAbsent(mode, key -> new ArrayList<>());
        for (BatchTask<?> batchTask : batchTaskList) {
            if (batchTask.getRespond() != null && batchTask.getRespond().getClass().isAssignableFrom(respondType)) {
                batchTaskList.remove(batchTask);
                Log.d("SQLBatch", "obtain");
                return batchTask;
            }
        }
        return new BatchTask<>(mode);
    }

    /**
     * 批量插入数据，数据属于同一个表的只会触发一次文件写入的操作。
     *
     * @param tableName     表名
     * @param beansToInsert 需要批量插入的数据
     * @param thenTask      写入一次文件后需要执行的任务
     */
    public void insertBatch(String tableName, List<Bean> beansToInsert, ThenTask<Boolean> thenTask) {
        long batchStart = new Date().getTime();
        final BatchTask<Boolean> insertTask = new BatchTask<>(tableName, () -> {
            Log.d(TAG, "INSERT BATCH");
            // 0)先对所有的bean分表，同一个表的插入只需要一次IO
            Map<File, List<Bean>> batchSubTables = new HashMap<>();
            File subTableFile;
            for (Bean bean : beansToInsert) {
                if (isEmpty(bean.getKey())) {
                    subTableFile = sqlService.getSubTableFileOrCreate(tableName, null);
                } else {
                    subTableFile = sqlService.getSubTableFileOrCreate(tableName, sqlService.getKeyId(bean.getKey()));
                }
                batchSubTables.computeIfAbsent(subTableFile, key -> new ArrayList<>()).add(bean);
            }
            int size = batchSubTables.size() - 1;
            // 1)创建每个分表的批处理任务
            batchSubTables.forEach((file, beans) -> {
                BatchTask<Boolean> task = (BatchTask<Boolean>) obtain(BatchMode.INSERT, Boolean.class);
                task.setTableName(tableName);
                task.setTask(() -> sqlService.insert(tableName, file, beans));
                task.start().then(null);
            });
            return true;
        }, BatchMode.INSERT, 0);
        // 2)开始执行批处理任务
        insertTask.start().then(respond -> {
            Log.d(TAG, "batch insert time = " + (new Date().getTime() - batchStart) + "ms");
            thenTask.then(respond);
        });
    }

    /**
     * 数据库更新的批处理操作。
     *
     * @param tableName 数据库表名
     * @param condition 自定义更新条件
     * @param operation 匹配更新条件的数据操作
     * @param thenTask  更新结束后的后续任务
     */
    public void updateBatch(String tableName, SQLService.Condition<Bean> condition,
                            SQLService.Operation<Bean> operation, ThenTask<Boolean> thenTask) {
        long batchStart = new Date().getTime();
        final BatchTask<Boolean> selectTask = new BatchTask<>(tableName, () -> {
            Log.d(TAG, "UPDATE BATCH");
            // 0)查找所有的子表文件
            File[] allSubTableFiles = sqlService.getAllSubTableFile(tableName);
            if (allSubTableFiles == null) return false;
            // 1)从每个子表文件中找出匹配更新条件的bean，并执行更新操作
            for (int i = 0; i < allSubTableFiles.length; i++) {
                File subTableFile = allSubTableFiles[i];
                List<Bean> subTableBeans = sqlService.getTableFileBeans(subTableFile);
                List<Bean> acceptBeans = new ArrayList<>();
                for (Bean bean : subTableBeans) {
                    if (condition.accept(bean)) {
                        operation.operate(bean);
                        acceptBeans.add(bean);
                    }
                }
                // 只有存在匹配的bean时才添加到待批处理的表中，避免创建大量空的批处理任务
                if (!acceptBeans.isEmpty()) {
                    // 2)为匹配更新条件的beans创建批处理任务
                    BatchTask<Boolean> task = (BatchTask<Boolean>) obtain(BatchMode.UPDATE, Boolean.class);
                    task.setTableName(tableName);
                    task.setDelay(10);
                    task.setTask(() -> sqlService.update(tableName, subTableFile, subTableBeans, acceptBeans));
                    task.start().then(null);
                }
            }
            return true;
        }, BatchMode.SELECT, 10);
        selectTask.start().then(respond -> {
            Log.d(TAG, "batch update time = " + (new Date().getTime() - batchStart) + "ms");
            thenTask.then(respond);
        });
    }

    /**
     * 批量查询数据。
     *
     * @param tableName 查询的数据表名
     * @param condition 自定义查询条件
     * @param thenTask  查询后的后续任务
     */
    public void selectBatch(String tableName, SQLService.Condition<Bean> condition,
                            ThenTask<ArrayList<Bean>> thenTask) {
        long batchStart = new Date().getTime();
        final BatchTask<ArrayList<Bean>> selectTask = new BatchTask<>(tableName, () -> {
            Log.d(TAG, "SELECT BATCH");
            return sqlService.selectByCondition(tableName, condition);
        }, BatchMode.SELECT, 10);
        selectTask.start().then(respond -> {
            Log.d(TAG, "batch select time = " + (new Date().getTime() - batchStart) + "ms");
            thenTask.then(respond);
        });
    }

    /**
     * 批量删除数据。
     *
     * @param tableName 删除数据的表名
     * @param condition 自定义删除条件
     * @param thenTask  删除结束后的后续任务
     */
    public void deleteBatch(String tableName, SQLService.Condition<Bean> condition,
                            ThenTask<Boolean> thenTask) {
        long batchStart = new Date().getTime();
        final BatchTask<Boolean> deleteTask = new BatchTask<>(tableName, () -> {
            Log.d(TAG, "DELETE BATCH");
            // 0)查找所有的子表文件
            File[] allSubTableFiles = sqlService.getAllSubTableFile(tableName);
            if (allSubTableFiles == null) return false;
            // 1)从每个子表文件中找出匹配更新条件的bean，并执行更新操作
            for (int i = 0; i < allSubTableFiles.length; i++) {
                File subTableFile = allSubTableFiles[i];
                List<Bean> subTableBeans = sqlService.getTableFileBeans(subTableFile);
                List<Bean> acceptBeans = new ArrayList<>();
                for (Bean bean : subTableBeans) {
                    if (condition.accept(bean)) {
                        acceptBeans.add(bean);
                    }
                }
                // 只有存在匹配的bean时才添加到待批处理的表中，避免创建大量空的批处理任务
                if (!acceptBeans.isEmpty()) {
                    for (Bean acceptBean : acceptBeans) {
                        subTableBeans.remove(acceptBean);
                    }
                    // 2)为匹配删除条件的beans创建批处理任务
                    BatchTask<Boolean> task = (BatchTask<Boolean>) obtain(BatchMode.DELETE, Boolean.class);
                    task.setTableName(tableName);
                    task.setTask(() -> sqlService.delete(subTableFile, subTableBeans, acceptBeans));
                    task.start().then(null);
                }
            }
            return true;
        }, BatchMode.DELETE, 0);
        // 3)执行批处理任务
        deleteTask.start().then(respond -> {
            Log.d(TAG, "batch delete time = " + (new Date().getTime() - batchStart) + "ms");
            thenTask.then(respond);
        });
    }

    private boolean isEmpty(String s) {
        return s == null || s.equals("");
    }
}
