package top.totoro.sql.clap;

import com.sun.istack.internal.NotNull;
import com.sun.istack.internal.Nullable;

import java.io.File;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * 数据库的批处理能力。
 * 创建时间 2020/7/15
 *
 * @author dragon
 * @version 1.0
 */
@SuppressWarnings("ALL")
public class SQLBatch<Bean extends SQLBean> {

    private static final String TAG = "SQLBatch";
    private final SQLService<Bean> sqlService;

    public SQLBatch(SQLService<Bean> sqlService) {
        this.sqlService = sqlService;
    }

    // 使用机器的处理器数量创建计划执行的的服务
    protected static final ScheduledExecutorService SCHEDULED_EXECUTOR
            = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
    // 存储当前存在的所有批处理对象优先级集合，执行批处理任务时需要根据优先级执行
    protected static final Map<BatchTask.BatchMode, List<BatchTask<? extends Serializable>>> BATCH_PRIORITY_MAP
            = new ConcurrentHashMap<>();
    // 当前可再利用的批处理空对象
    protected static final Map<BatchTask.BatchMode, List<BatchTask<? extends Serializable>>> BATCH_AVAILABLE_MAP
            = new ConcurrentHashMap<>();

    private int i = 0;

    private synchronized BatchTask<?> obtain(BatchTask.BatchMode mode, Class<?> respondType) {
        List<BatchTask<?>> batchTaskList = BATCH_AVAILABLE_MAP.computeIfAbsent(mode, key -> new ArrayList<>());
        for (BatchTask<?> batchTask : batchTaskList) {
            if (batchTask.getRespond() != null && batchTask.getRespond().getClass().isAssignableFrom(respondType)) {
                batchTaskList.remove(batchTask);
                return batchTask;
            }
        }
        return new BatchTask<>();
    }

    /**
     * 批量插入数据，数据属于同一个表的只会触发一次文件写入的操作。
     *
     * @param tableName     表名
     * @param beansToInsert 需要批量插入的数据
     * @param thenTask      写入一次文件后需要执行的任务
     */
    public void insertBatch(String tableName, @NotNull List<Bean> beansToInsert, @Nullable BatchTask.ThenTask<Boolean> thenTask) {
        long batchStart = new Date().getTime();
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
            BatchTask<Boolean> task = (BatchTask<Boolean>) obtain(BatchTask.BatchMode.INSERT, Boolean.class);
            task.setMode(BatchTask.BatchMode.INSERT);
            task.setDelay(0);
            task.setTask(() -> sqlService.insert(file, beans));
            // 2)开始执行批处理任务
            i++;
            if (i == size) {
                task.start().then(respond -> Log.d(TAG, "batch insert time = " + (new Date().getTime() - batchStart) + "ms"));
            }
            task.start().then(thenTask);
        });
    }

    public void updateBatch(String tableName, @NotNull SQLService.Condition<Bean> condition,
                            @NotNull SQLService.Operation<Bean> operation, @Nullable BatchTask.ThenTask<Boolean> thenTask) {
        long batchStart = new Date().getTime();
        // 0)查找所有的子表文件
        File[] allSubTableFiles = sqlService.getAllSubTableFile(tableName);
        if (allSubTableFiles == null) return;
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
                BatchTask<Boolean> task = (BatchTask<Boolean>) obtain(BatchTask.BatchMode.UPDATE, Boolean.class);
                task.setMode(BatchTask.BatchMode.UPDATE);
                task.setDelay(0);
                task.setTask(() -> sqlService.update(subTableFile, subTableBeans, acceptBeans));
//                if (i == allSubTableFiles.length - 1) {
//                    task.start().then(respond -> Log.d(TAG, "batch update time = " + (new Date().getTime() - batchStart) + "ms"));
//                }
                task.start().then(thenTask);
            }
        }
    }

    public void selectBatch(String tableName, @NotNull SQLService.Condition<Bean> condition,
                            @Nullable BatchTask.ThenTask<ArrayList<Bean>> thenTask) {
        long batchStart = new Date().getTime();
        Object objTask = obtain(BatchTask.BatchMode.SELECT, ArrayList.class);
        BatchTask<ArrayList<Bean>> task = (BatchTask<ArrayList<Bean>>) obtain(BatchTask.BatchMode.SELECT, ArrayList.class);
        task.setMode(BatchTask.BatchMode.SELECT);
        task.setDelay(0);
        task.setTask(() -> sqlService.selectByCondition(tableName, condition));
//        task.start().then(respond -> Log.d(TAG, "batch select time = " + (new Date().getTime() - batchStart) + "ms"));
        task.start().then(thenTask);
    }

    public void deleteBatch(String tableName, @NotNull SQLService.Condition<Bean> condition,
                            @Nullable BatchTask.ThenTask<Boolean> thenTask) {
        long batchStart = new Date().getTime();
        // 0)查找所有的子表文件
        File[] allSubTableFiles = sqlService.getAllSubTableFile(tableName);
        if (allSubTableFiles == null) return;
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
                BatchTask<Boolean> task = (BatchTask<Boolean>) obtain(BatchTask.BatchMode.DELETE, Boolean.class);
                task.setMode(BatchTask.BatchMode.DELETE);
                task.setDelay(0);
                task.setTask(() -> sqlService.delete(subTableFile, subTableBeans, acceptBeans));
                // 3)执行批处理任务
                if (i == allSubTableFiles.length - 1) {
                    task.start().then(respond -> Log.d(TAG, "batch delete time = " + (new Date().getTime() - batchStart) + "ms"));
                }
                task.start().then(thenTask);
            }
        }
    }

    private boolean isEmpty(String s) {
        return s == null || s.equals("");
    }
}
