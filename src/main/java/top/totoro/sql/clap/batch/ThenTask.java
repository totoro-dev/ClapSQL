package top.totoro.sql.clap.batch;

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