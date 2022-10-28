package cn.beichenhpy;

import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.date.StopWatch;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author beichenhpy
 * <p> 2022/10/28 22:41
 */
@Slf4j
public class TaskHandler {

    public void doSomething(List<String> taskList) {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = ThreadPoolConfig.threadPoolHandleTaskExecutor();
        //分配处理线程池的 max + queue 可以充分利用线程，防止进入拒绝策略
        List<List<String>> taskGroup = ListUtil.split(taskList, threadPoolTaskExecutor.getMaxPoolSize() + threadPoolTaskExecutor.getQueueSize());
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        for (List<String> taskMembers : taskGroup) {
            for (String task : taskMembers) {
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        Thread.sleep(5000);
                        log.info("执行完任务 :{}", task);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }, threadPoolTaskExecutor).exceptionally(e -> {
                    log.error("异常: {}, {}", e.getMessage(), e);
                    return null;
                });
                log.debug("分发task: {}, 完毕", task);
                futures.add(future);
            }
            //阻塞每组任务线程 防止超发
            log.debug("阻塞每组任务线程 防止超发");
            CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).join();
            log.debug("每组任务线程执行结束");
        }
        stopWatch.stop();
        log.info("任务执行结束了，花费时间: {}", stopWatch.getTotalTimeSeconds());
        threadPoolTaskExecutor.shutdown();
    }

    public static void main(String[] args) {
        doWork(1);
        doWork(2);
        doWork(3);
        doWork(4);
        doWork(5);
        doWork(6);
    }

    public static void doWork(int batch) {
        List<String> taskList = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            taskList.add(batch + "--task--" + i);
        }
        TaskHandler taskHandler = new TaskHandler();
        ThreadPoolTaskExecutor dispatch = ThreadPoolConfig.dispatchThreadPool;
        dispatch.submit(
                () -> taskHandler.doSomething(taskList)
        );
    }
}
