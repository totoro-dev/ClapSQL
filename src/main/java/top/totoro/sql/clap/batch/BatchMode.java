package top.totoro.sql.clap.batch;

/**
 * 批处理任务的执行模式，对用这数据库的增删改查，并且具有任务优先级。
 * 优先级 ： 插入 》 更新 》 删除 》 查询
 */
public enum BatchMode {
    /* 批处理的模式 */
    // 按优先级顺序
    INSERT, UPDATE, DELETE, SELECT,
}
