package top.totoro.sql.clap;

import java.util.concurrent.*;

/**
 * 数据库的批处理能力。
 * 创建时间 2020/7/15
 *
 * @author dragon
 * @version 1.0
 */
public class SQLBatch<Bean extends SQLBean> {
    /* 批处理的模式 */
    public enum BatchMode {
        INSERT, SELECT, UPDATE, DELETE
    }

    /* 批处理任务 */
    public class BatchTask<Request, Respond> {
        // 用于处理任务
        private Callable<Respond> mTask;
        private ThenTask<Respond> mThenRun;
        // 当前任务的处理模式
        private BatchMode mMode;
        // 从开启任务到真正执行的延迟
        private long mDelay;
        // 当前任务执行所需的参数
        private Request mRequest;
        // 当前任务的执行结果
        private ScheduledFuture<Respond> mRespond;

        /* 正式开启批处理，可以链式调用继续异步执行then方法 */
        public BatchTask<Request, Respond> start() {
            mRespond = SCHEDULED_EXECUTOR.schedule(mTask, mDelay, TimeUnit.MILLISECONDS);
            return this;
        }

        /* 批处理执行结束后继续异步执行后续任务then */
        public void then(ThenTask<Respond> then) {
            Runnable thenRun = () -> {
                try {
                    Respond respond = mRespond.get();
                    then.then(respond);
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            };
            SCHEDULED_EXECUTOR.schedule(thenRun, 0, TimeUnit.MICROSECONDS);
        }

    }

    public interface ThenTask<Respond> {
        void then(Respond respond);
    }

    // 使用机器的处理器数量创建计划执行的的服务
    private static final ScheduledExecutorService SCHEDULED_EXECUTOR = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());

    public boolean process() {
        return false;
    }
}
