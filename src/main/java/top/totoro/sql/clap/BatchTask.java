package top.totoro.sql.clap;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static top.totoro.sql.clap.SQLBatch.BATCH_PRIORITY_MAP;
import static top.totoro.sql.clap.SQLBatch.SCHEDULED_EXECUTOR;

/**
 * 一个可执行的批处理任务，可以通过obtain获取批处理任务对象，确保任务优先级。
 * 创建时间 2020/7/16
 *
 * @param <Respond> 批处理任务的返回类型
 * @author dragon
 * @version 1.0
 */
public class BatchTask<Respond extends Serializable> {
    private static final String TAG = "BatchTask";

    /**
     * 批处理任务的执行模式，对用这数据库的增删改查，并且具有任务优先级。
     * 优先级 ： 插入 》 更新 》 删除 》 查询
     */
    /* 批处理的模式 */
    public enum BatchMode {
        // 按优先级顺序
        INSERT, UPDATE, DELETE, SELECT,
    }

    private String mTableName = "";
    // 用于处理任务
    private Callable<Respond> mTask;
    // 当前任务的处理模式
    private BatchMode mMode;
    // 从开启任务到真正执行的延迟
    private long mDelay = 0;
    // 当前任务的执行Future
    private ScheduledFuture<Respond> mRespondFuture;
    // 当前任务的执行结果
    private Respond mRespond;

    /**
     * 设置该任务执行操作对应的是那张表的表名。
     *
     * @param tableName 操作对应的表名
     */
    public void setTableName(String tableName) {
        this.mTableName = tableName;
    }

    public Callable<Respond> getTask() {
        return mTask;
    }

    /**
     * 设置批处理具体执行的任务
     *
     * @param task 异步任务
     */
    public void setTask(Callable<Respond> task) {
        this.mTask = task;
    }

    public BatchMode getMode() {
        return mMode;
    }

    /**
     * 设置任务的执行模式{@link BatchMode}
     *
     * @param mode 执行模式
     */
    public void setMode(BatchMode mode) {
        this.mMode = mode;
    }

    public long getDelay() {
        return mDelay;
    }

    /**
     * 设置从开启任务到真正执行任务的延迟（ms）
     *
     * @param delay 执行延迟
     */
    public void setDelay(long delay) {
        this.mDelay = delay;
    }

    /**
     * 获取任务当前的结果
     *
     * @return 执行结果，为执行结束之前为空。
     */
    public Respond getRespond() {
        return mRespond;
    }

    public BatchTask() {
    }

    public BatchTask(BatchMode mode) {
        this.mMode = mode;
    }

    public BatchTask(String tableName, Callable<Respond> task, BatchMode mode, long delay) {
        this.mTableName = tableName;
        this.mTask = task;
        this.mMode = mode;
        this.mDelay = delay;
    }

    /**
     * 开启批处理任务，需要直接设置任务参数。
     *
     * @param tableName 任务在那个数据库表中执行。
     * @param task      执行的具体任务，在线程中执行。
     * @param mode      执行的任务属于那种模式。
     * @param delay     从开启到尝试执行的延迟时间。
     * @return 当前任务，可以继续链式调用。
     */
    /* 正式开启批处理，可以链式调用继续异步执行then方法 */
    public BatchTask<Respond> start(String tableName, Callable<Respond> task, BatchMode mode, long delay) {
        mTableName = tableName;
        mTask = task;
        mMode = mode;
        mDelay = delay;
        return start();
    }

    /**
     * 开启当前批处理任务
     *
     * @return 当前批处理任务，可以继续链式调用
     */
    /* 正式开启批处理，可以链式调用继续异步执行then方法 */
    public BatchTask<Respond> start() {
        assert mTask != null;
        HashMap<BatchMode, LinkedList<BatchTask<? extends Serializable>>> tableTaskMap = BATCH_PRIORITY_MAP.computeIfAbsent(mTableName, key -> new HashMap<>());
        tableTaskMap.computeIfAbsent(getMode(), table -> new LinkedList<>()).add(this);
        mRespondFuture = SCHEDULED_EXECUTOR.schedule(() -> {
            waitToExecutor(getMode());
            return mTask.call();
        }, mDelay, TimeUnit.MILLISECONDS);
        return this;
    }

    private void waitToExecutor(BatchTask.BatchMode mode) {
        if (mode == BatchTask.BatchMode.INSERT) return;
        while (true) {
            HashMap<BatchMode, LinkedList<BatchTask<? extends Serializable>>> tableTaskMap = BATCH_PRIORITY_MAP.computeIfAbsent(mTableName, key -> new HashMap<>());
            List<BatchTask<?>> insertTasks = tableTaskMap.get(BatchMode.INSERT);
            boolean insertTaskEmpty = insertTasks == null || insertTasks.isEmpty();
            if (insertTaskEmpty) {
                if (mode == BatchTask.BatchMode.UPDATE) return;
            }
            List<BatchTask<?>> updateTasks = tableTaskMap.get(BatchMode.UPDATE);
            boolean updateTaskEmpty = updateTasks == null || updateTasks.isEmpty();
            if (insertTaskEmpty && updateTaskEmpty) {
                if (mode == BatchTask.BatchMode.DELETE) return;
            }
            List<BatchTask<?>> deleteTasks = tableTaskMap.get(BatchMode.DELETE);
            boolean deleteTaskEmpty = deleteTasks == null || deleteTasks.isEmpty();
            if (insertTaskEmpty && updateTaskEmpty && deleteTaskEmpty) {
                return;
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 当批处理任务的线程执行结束之后，开启新的线程执行指定的后续任务。
     *
     * @param then 后续要执行的任务，对于调用者来说都是异步的，
     *             但是对于批处理来说是同步的，因为后续任务必须到批处理任务结束才会执行。
     */
    /* 批处理执行结束后继续异步执行后续任务then */
    public void then(ThenTask<Respond> then) {
        Runnable thenRun = () -> {
            try {
                mRespond = mRespondFuture.get();
                BATCH_PRIORITY_MAP.get(mTableName).computeIfAbsent(getMode(), mode -> new LinkedList<>()).remove(this);
                if (then == null) return;
                then.then(mRespond);
//                BATCH_AVAILABLE_MAP.get(getMode()).add(this);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        };
        SCHEDULED_EXECUTOR.schedule(thenRun, 0, TimeUnit.MICROSECONDS);
    }

    /**
     * 批处理任务执行结束后要要执行的自定义任务。
     * 每个不同的需求都可以实现自定义的后续任务，确保数据库操作已完成。
     *
     * @param <Respond> 批处理任务的返回结果的类型。
     */
    public interface ThenTask<Respond> {
        /**
         * 在这个方法中获取到批处理任务的执行结果，并实现自己的后续任务。
         *
         * @param respond 批处理任务的执行结果。
         */
        void then(Respond respond);
    }
}

