package cn.beichenhpy;

import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author beichenhpy
 * <p> 2022/10/28 22:37
 */
public class ThreadPoolConfig {

    //调度线程池一般为单例 内存共享
    public static final ThreadPoolTaskExecutor dispatchThreadPool = threadPoolDispatchTaskExecutor();

    private static ThreadPoolTaskExecutor threadPoolDispatchTaskExecutor() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setCorePoolSize(5);
        threadPoolTaskExecutor.setMaxPoolSize(10);
        threadPoolTaskExecutor.setQueueCapacity(10);
        threadPoolTaskExecutor.setThreadNamePrefix("all-task-dispatch-");
        threadPoolTaskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        threadPoolTaskExecutor.initialize();
        return threadPoolTaskExecutor;
    }

    //一般为每次执行创建，防止进入拒绝策略
    public static ThreadPoolTaskExecutor threadPoolHandleTaskExecutor() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setCorePoolSize(5);
        threadPoolTaskExecutor.setMaxPoolSize(10);
        threadPoolTaskExecutor.setQueueCapacity(10);
        threadPoolTaskExecutor.setThreadNamePrefix("task-handler-");
        threadPoolTaskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        threadPoolTaskExecutor.initialize();
        return threadPoolTaskExecutor;
    }
}
