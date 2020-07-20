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
 * 一个可执行的批处理任务，需要通过obtain获取批处理队列，确保任务优先级。
 * 创建时间 2020/7/16
 *
 * @param <Respond> 批处理任务的返回类型
 * @author dragon
 * @version 1.0
 */
public class BatchTask<Respond extends Serializable> {
    private static final String TAG = "BatchTask";

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
    // 当前任务的执行结果
    private ScheduledFuture<Respond> mRespondFuture;
    private Respond mRespond;

    public void setTableName(String tableName) {
        this.mTableName = tableName;
    }

    public Callable<Respond> getTask() {
        return mTask;
    }

    public void setTask(Callable<Respond> task) {
        this.mTask = task;
    }

    public BatchMode getMode() {
        return mMode;
    }

    public void setMode(BatchMode mode) {
        this.mMode = mode;
    }

    public long getDelay() {
        return mDelay;
    }

    public void setDelay(long delay) {
        this.mDelay = delay;
    }

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

    /* 正式开启批处理，可以链式调用继续异步执行then方法 */
    public BatchTask<Respond> start(String tableName, Callable<Respond> task, BatchMode mode, long delay) {
        mTableName = tableName;
        mTask = task;
        mMode = mode;
        mDelay = delay;
        return start();
    }

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

    public interface ThenTask<Respond> {
        void then(Respond respond);
    }
}

