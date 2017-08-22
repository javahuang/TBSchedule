package me.hrps.schedule.strategy;

import com.google.common.collect.Maps;
import me.hrps.schedule.ScheduleUtils;
import me.hrps.schedule.config.TBScheduleConfig;
import me.hrps.schedule.taskmanager.IScheduleDataManager;
import me.hrps.schedule.taskmanager.TBScheduleManagerStatic;
import me.hrps.schedule.taskmanager.TBScheduledTaskProcessor;
import me.hrps.schedule.zk.ScheduleDataManager4ZK;
import me.hrps.schedule.zk.ScheduleStrategyDataManager4ZK;
import me.hrps.schedule.zk.ZKManager;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Description:
 * <pre>
 * </pre>
 * Author: huangrupeng
 * Create: 17/7/6 下午10:11
 */
public class TBScheduleManagerFactory implements ApplicationContextAware, SmartInitializingSingleton, BeanFactoryAware, BeanNameAware {
    private static transient Logger logger = LoggerFactory.getLogger(TBScheduleManagerFactory.class);

    private ApplicationContext applicationContext;
    private BeanFactory beanFactory;
    private String beanName;

    private TBScheduleConfig config;
    private String uuid;
    private String ip;
    private String hostName;

    protected Lock lock = new ReentrantLock();
    private InitialThread initialThread;
    private ZKManager zkManager;
    private Timer timer;
    private ManagerFactoryTimerTask timerTask;

    volatile String errorMessage = "No config Zookeeper connect infomation";


    /**
     * 调度配置中心客户端
     */
    private IScheduleDataManager scheduleDataManager;
    private ScheduleStrategyDataManager4ZK scheduleStrategyManager;

    private Map<String, List<IStrategyTask>> managerMap = Maps.newConcurrentMap();

    /**
     * 是否启动调度管理
     */
    public boolean start = true;
    private int timeInterval = 2000;

    public TBScheduleManagerFactory(TBScheduleConfig config) {
        this.config = config;
        this.ip = ScheduleUtils.getLocalIp();
        this.hostName = ScheduleUtils.getHostName();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
        if (this.beanFactory == null) {
            this.beanFactory = applicationContext;
        }
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
    }

    @Override
    public void setBeanName(String name) {
        this.beanName = name;
    }

    public String getBeanName() {
        return beanName;
    }

    public Object getBean(String beanName) {
        return applicationContext.getBean(beanName);
    }

    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    public BeanFactory getBeanFactory() {
        return beanFactory;
    }

    /**
     * 执行 zookeeper 的初始化
     *
     * @throws Exception
     */
    public void init() throws Exception {
        if (this.initialThread != null) {
            this.initialThread.stopThread();
        }
        this.lock.lock();
        try {
            if (this.zkManager != null) {
                this.zkManager.close();
            }
            this.zkManager = new ZKManager(config);
            initialThread = new InitialThread(this);
            initialThread.setName("TBScheduleManagerFactory-initialThread");
            initialThread.start();
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * 所有单例 bean 创建完毕，开始执行初始化 TBSchedule
     */
    @Override
    public void afterSingletonsInstantiated() {
        try {
            logger.info("开始初始化 TBSchedule...");
            init();
        } catch (Exception e) {
            logger.error("TBSchedule 初始化失败");
        }
    }

    /**
     * 在 zookeeper 初始化以后执行数据的初始化
     * <p>
     * 创建 zookeeper task/strategy 节点
     * </p>
     */
    public void initialData() throws Exception {
        this.zkManager.initial();
        this.scheduleDataManager = new ScheduleDataManager4ZK(this.zkManager);
        this.scheduleStrategyManager = new ScheduleStrategyDataManager4ZK(this.zkManager);
        if (this.start) {
            // 注册调度管理器
            this.scheduleStrategyManager.registerManagerFactory(this);
            // 创建注解任务处理器
            new TBScheduledTaskProcessor(this, this.scheduleStrategyManager);
            if (timer == null) {
                timer = new Timer("TBScheduleManagerFactory-Timer");
            }
            if (timerTask == null) {
                timerTask = new ManagerFactoryTimerTask(this);
                timer.schedule(timerTask, 2000, this.timeInterval);
            }

        }
    }

    public void refresh() throws Exception {
        this.lock.lock();
        try {
            ManagerFactoryInfo stsInfo = null;
            boolean isException = false;
            try {
                stsInfo = this.scheduleStrategyManager.loadManagerFactoryInfo(getUuid());
            } catch (Exception e) {
                isException = true;
                logger.error("获取服务器信息有误，uuid：" + getUuid(), e);
            }
            if (isException) {
                try {
                    stopServer(null);
                    this.scheduleStrategyManager.unRegisterManagerFactory(this);
                } finally {
                    reRegisterManagerFactory();
                }
            } else if (!stsInfo.isStart()) {
                stopServer(null);
                this.scheduleStrategyManager.unRegisterManagerFactory(this);
            } else {
                reRegisterManagerFactory();
            }

        } finally {
            this.lock.unlock();
        }
    }

    public void reRegisterManagerFactory() throws Exception {
        List<String> stopList = this.scheduleStrategyManager.registerManagerFactory(this);
        for (String strategyName : stopList) {
            this.stopServer(strategyName);
        }
        this.assignScheduleServer();
        this.reRunScheduleServer();
    }

    /**
     * 根据策略重新分配调度任务的机器
     * 根据配置的线程组数量，平均分配到每个注册到 strategy 上的 worker
     */
    private void assignScheduleServer() throws Exception {
        for (ScheduleStrategyRuntime run : this.scheduleStrategyManager.loadAllStrategyRuntimeByUUID(this.getUuid())) {
            List<ScheduleStrategyRuntime> factoryList = this.scheduleStrategyManager.loadAllScheduleStrategyRuntimeByTaskType(run.getStrategyName());
            if (factoryList.size() == 0 || !this.isLeader(this.uuid, factoryList)) {
                continue;
            }
            ScheduleStrategy scheduleStrategy = this.scheduleStrategyManager.loadScheduleStrategy(run.getStrategyName());

            int[] nums = ScheduleUtils.assignTaskNumber(factoryList.size(), scheduleStrategy.getAssignNum(), scheduleStrategy.getNumOfSingleServer());
            for (int i = 0; i < factoryList.size(); i++) {
                ScheduleStrategyRuntime factoryRunTime = factoryList.get(i);
                this.scheduleStrategyManager.updateStrategyRuntimeRequestNum(run.getStrategyName(), factoryRunTime.getUuid(), nums[i]);
            }
        }
    }

    /**
     * 判断策略下面的某个 factory 是否是 leader
     *
     * @param uuid
     * @param factoryList
     * @return
     */
    public boolean isLeader(String uuid, List<ScheduleStrategyRuntime> factoryList) {
        int index = Integer.parseInt(StringUtils.substringAfterLast(uuid, "$"));
        for (ScheduleStrategyRuntime factoryOfStrategy : factoryList) {
            int currIndex = Integer.parseInt(StringUtils.substringAfterLast(factoryOfStrategy.getUuid(), "$"));
            if (index > currIndex) {
                return false;
            }
        }
        return true;
    }

    /**
     * 根据运行时策略分配对应数量的线程组
     *
     * @throws Exception
     */
    public void reRunScheduleServer() throws Exception {
        // 所有包含当前 worker 的 strategy
        for (ScheduleStrategyRuntime run : this.scheduleStrategyManager.loadAllStrategyRuntimeByUUID(this.uuid)) {
            List<IStrategyTask> list = this.managerMap.computeIfAbsent(run.getStrategyName(), k -> new ArrayList<>());
            while (list.size() > run.getRequestNum() && list.size() > 0) {
                IStrategyTask task = list.remove(list.size() - 1);
                try {
                    task.stop(run.getStrategyName());
                } catch (Exception e) {
                    logger.error("注销任务失败：strategyName=", run.getStrategyName(), e);
                }
            }
            // 不足，增加调度器
            ScheduleStrategy strategy = this.scheduleStrategyManager.loadScheduleStrategy(run.getStrategyName());
            while (list.size() < run.getRequestNum()) {
                IStrategyTask result = this.createStrategyTask(strategy);
                if (result == null) {
                    logger.error("strategy 对应的配置有问题。strategy name=" + strategy.getStrategyName());
                }
                list.add(result);
            }
        }
    }

    /**
     * 创建策略任务
     *
     * @param strategy
     * @return
     */
    public IStrategyTask createStrategyTask(ScheduleStrategy strategy) {
        IStrategyTask task = null;
        try {
            if (ScheduleStrategy.Kind.Schedule == strategy.getKind()) {
                String baseTaskType = ScheduleUtils.getBaseTaskTypeFromTaskType(strategy.getTaskName());
                String ownSign = ScheduleUtils.getOwnSignFromTaskType(strategy.getTaskName());
                task = new TBScheduleManagerStatic(this, baseTaskType, ownSign, scheduleDataManager);
            }
        } catch (Exception e) {
            logger.error("strategy 获取对应的java or bean 出错,schedule并没有加载该任务,请确认" + strategy.getStrategyName(), e);
        }
        return task;
    }


    /**
     * 终止任务
     *
     * @param strategyName
     */
    private void stopServer(String strategyName) {
        this.managerMap.keySet().stream().filter(x -> (strategyName != null && x.equalsIgnoreCase(strategyName)) || strategyName == null)
                .forEach(name -> {
                    this.managerMap.get(name).forEach(task -> {
                        try {
                            task.stop(strategyName);
                        } catch (Exception e) {
                            logger.error("注销任务错误：strategyName={}", strategyName, e);
                        }
                    });
                    this.managerMap.remove(name);
                });
    }

    /**
     * 重启所有的服务
     */
    private void restart() {
        try {
            if (timer != null) {
                if (timerTask != null) {
                    timerTask.cancel();
                    timerTask = null;
                }
                timer.purge();
            }
            stopServer(null);
            if (zkManager != null) {
                zkManager.close();
            }
            this.uuid = null;
            this.init();
        } catch (Exception e) {
            logger.error("重启服务失败", e);
        }
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getIp() {
        return ip;
    }

    public String getHostName() {
        return hostName;
    }

    public ZKManager getZkManager() {
        return zkManager;
    }

    /**
     * 初始化线程
     */
    static class InitialThread extends Thread {
        private static transient Logger logger = LoggerFactory.getLogger(InitialThread.class);

        TBScheduleManagerFactory facotry;
        boolean isStop = false;

        public InitialThread(TBScheduleManagerFactory facotry) {
            this.facotry = facotry;
        }

        public void stopThread() {
            isStop = true;
        }

        @Override
        public void run() {
            facotry.lock.lock();
            try {
                int count = 0;
                while (!facotry.zkManager.isZkConnected()) {
                    count++;
                    if (count % 50 == 0) {
                        facotry.errorMessage = "connecting zookeeper,spend time " + count * 20 + "ms";
                        logger.error(facotry.errorMessage);
                    }
                    Thread.sleep(20);
                    if (this.isStop) {
                        return;
                    }
                }
                facotry.initialData();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            } finally {
                facotry.lock.unlock();
            }
            super.run();
        }
    }

    static class ManagerFactoryTimerTask extends TimerTask {
        private static transient Logger logger = LoggerFactory.getLogger(ManagerFactoryTimerTask.class);

        TBScheduleManagerFactory factory;
        int count = 0;

        public ManagerFactoryTimerTask(TBScheduleManagerFactory factory) {
            this.factory = factory;
        }

        @Override
        public void run() {
            Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
            try {
                if (!this.factory.zkManager.isZkConnected()) {
                    if (count > 5) {
                        logger.error("Zookeeper 连接失败，关闭所有任务后，重新连接 Zookeeper 服务器.....");
                        this.factory.restart();
                    } else {
                        count += 1;
                    }
                } else {
                    count = 0;
                    this.factory.refresh();
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            } finally {
            }
        }
    }
}
