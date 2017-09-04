package me.hrps.schedule.taskmanager;

import com.google.common.collect.Maps;
import me.hrps.schedule.CronExpression;
import me.hrps.schedule.strategy.TBScheduleManagerFactory;
import me.hrps.schedule.zk.ScheduleStrategyDataManager4ZK;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.config.NamedBeanHolder;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;

import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;

/**
 * Description:
 * <pre>
 *    被 @{@link me.hrps.schedule.config.annotation.TBScheduled} 注解的任务处理器
 * </pre>
 * Author: huangrupeng
 * Create: 17/8/13 下午6:48
 */
public class TBScheduledTaskProcessor implements Runnable {

    Logger logger = LoggerFactory.getLogger(TBScheduledTaskProcessor.class);

    TBScheduleManagerFactory factory;
    BeanFactory beanFactory;
    TaskScheduler taskScheduler;
    ScheduleStrategyDataManager4ZK strategyDataManager;
    public static final String DEFAULT_TASK_SCHEDULER_BEAN_NAME = "taskScheduler";
    // 默认调度线程池线程数量
    public static int core_pool_size = 2;

    Map<String, ScheduledFuture> scheduledResult = Maps.newConcurrentMap();

    public TBScheduledTaskProcessor(TBScheduleManagerFactory factory, ScheduleStrategyDataManager4ZK strategyDataManager) {
        this.factory = factory;
        this.beanFactory = factory.getBeanFactory();
        this.strategyDataManager = strategyDataManager;
        getTaskScheduler();
        new Thread(this).start();
    }

    @Override
    public void run() {
        try {
            // 注册任务
            Set<ScheduledMethodRunnable> tasks = strategyDataManager.registerScheduledTasks(factory, this);
            // 开始任务调度
            for (ScheduledMethodRunnable task : tasks) {
                scheduleTask(task);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 开始任务调度
     *
     * @param task
     */
    public void scheduleTask(ScheduledMethodRunnable task) {
        // 不允许多个线程同时调度一个任务
        synchronized (task) {
            // 显示错误信息
            if (task.getMsg() != null) {
                refreshTaskRunningInfo(task);
                return;
            }
            try {
                String taskName = task.getTaskName();
                if (task.isRunning()) {
                    taskScheduler.schedule(task, new Date());
                    return;
                }
                CronExpression cronExp = new CronExpression(task.getCron());
                Date startTime = cronExp.getNextValidTimeAfter(new Date());
                ScheduledFuture<?> future = taskScheduler.schedule(task, startTime);
                scheduledResult.put(taskName, future);
            } catch (ParseException p) {    // cron 表达式解析失败
                task.setMsg("cron表达式错误");
                refreshTaskRunningInfo(task);
                logger.error("任务{}cron 表达式设置错误", task.getTaskName(), p);
            } catch (Exception e) {
                logger.error("任务{}调度失败", task.getTaskName(), e);
            }
        }
    }

    private void getTaskScheduler() {
        try {
            this.taskScheduler = resolveSchedulerBean(TaskScheduler.class, false);
        } catch (Exception e) {
            // 调度任务线程数
            this.taskScheduler = new ConcurrentTaskScheduler(Executors.newScheduledThreadPool(core_pool_size));
        }
    }

    private <T> T resolveSchedulerBean(Class<T> schedulerType, boolean byName) {
        if (byName) {
            T scheduler = this.beanFactory.getBean(DEFAULT_TASK_SCHEDULER_BEAN_NAME, schedulerType);
            if (this.beanFactory instanceof ConfigurableBeanFactory) {
                ((ConfigurableBeanFactory) this.beanFactory).registerDependentBean(
                        DEFAULT_TASK_SCHEDULER_BEAN_NAME, factory.getBeanName());
            }
            return scheduler;
        } else if (this.beanFactory instanceof AutowireCapableBeanFactory) {
            NamedBeanHolder<T> holder = ((AutowireCapableBeanFactory) this.beanFactory).resolveNamedBean(schedulerType);
            if (this.beanFactory instanceof ConfigurableBeanFactory) {
                ((ConfigurableBeanFactory) this.beanFactory).registerDependentBean(
                        holder.getBeanName(), factory.getBeanName());
            }
            return holder.getBeanInstance();
        } else {
            return this.beanFactory.getBean(schedulerType);
        }
    }

    public void refreshTaskRunningInfo(ScheduledMethodRunnable task) {
        try {
            this.strategyDataManager.refreshScheduledTask(this.factory, task);
        } catch (Exception e) {
            logger.error("刷新运行时任务({})信息失败", task.getTaskName(), e);
        }
    }

    public Map<String, ScheduledFuture> getScheduledResult() {
        return scheduledResult;
    }


}