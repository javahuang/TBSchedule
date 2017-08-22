package me.hrps.schedule.taskmanager;

import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Date;

/**
 * Description:
 * <pre>
 *
 * </pre>
 * Author: huangrupeng
 * Create: 17/8/13 下午1:49
 */
public class ScheduledMethodRunnable implements Runnable {

    private transient final Object target;
    private transient TBScheduledTaskProcessor processor;
    private transient final Method method;
    private transient Object[] args = null;

    private String cron;
    private String taskName;
    private volatile boolean running = false;    // 是否正在运行
    private String argStr = null; // 方法参数,按照逗号分隔,只支持基本类型
    private long startTime;   // 任务开始运行时间
    private long endTime; // 任务结束运行时间
    private String msg;


    @Override
    public void run() {
        try {
            msg = null;
            running = true;
            processor.refreshTaskRunningInfo(this);
            parseArgStrToArgs();

            startTime = System.currentTimeMillis();
            ReflectionUtils.makeAccessible(this.method);
            this.method.invoke(this.target, args);
            endTime = System.currentTimeMillis();

            processor.refreshTaskRunningInfo(this);
            // 开始下次调度
            processor.scheduleTask(this);
            running = false;
        } catch (InvocationTargetException ex) {
            ReflectionUtils.rethrowRuntimeException(ex.getTargetException());
        } catch (IllegalAccessException ex) {
            throw new UndeclaredThrowableException(ex);
        } finally {
            running = false;
        }
    }

    public ScheduledMethodRunnable(Object target, Method method) {
        this.target = target;
        this.method = method;
    }

    public ScheduledMethodRunnable(Object target, String methodName) throws NoSuchMethodException {
        this.target = target;
        this.method = target.getClass().getMethod(methodName);
    }

    public ScheduledMethodRunnable(Object target, Method method, String cron) {
        this.target = target;
        this.method = method;
        this.cron = cron;
    }

    public Object getTarget() {
        return target;
    }

    public Method getMethod() {
        return method;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public Object[] getArgs() {
        return args;
    }

    public void setArgs(Object[] args) {
        this.args = args;
    }

    public boolean isRunning() {
        return running;
    }

    public TBScheduledTaskProcessor getProcessor() {
        return processor;
    }

    public void setProcessor(TBScheduledTaskProcessor processor) {
        this.processor = processor;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String getArgStr() {
        return argStr;
    }

    public void setArgStr(String argStr) {
        this.argStr = argStr;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    /**
     * 将方法参数字符串解析为方法参数
     */
    public void parseArgStrToArgs() {
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes != null && parameterTypes.length > 0) {
            args = new Object[parameterTypes.length];
            try {
                String[] parametersStr = null;
                boolean needSetParameter = false;
                if (StringUtils.hasText(argStr)) {
                    parametersStr = argStr.split("\\s+");
                    if (parametersStr.length == parameterTypes.length) {
                        needSetParameter = true;
                    } else {
                        setMsg("参数设置异常");
                    }
                }
                for (int i = 0; i < parameterTypes.length; i++) {
                    Class<?> parameterType = parameterTypes[i];
                    String parameterTypeName = parameterType.getName();
                    if ("int".equals(parameterTypeName)) {
                        args[i] = needSetParameter ? new Integer(parametersStr[i]) : 0;
                    } else if (parameterTypeName.contains("Integer")) {
                        args[i] = needSetParameter ? new Integer(parametersStr[i]) : null;
                    } else if ("double".equals(parameterTypeName)) {
                        args[i] = needSetParameter ? new Double(parametersStr[i]) : 0;
                    } else if (parameterTypeName.contains("Double")) {
                        args[i] = needSetParameter ? new Double(parametersStr[i]) : null;
                    } else if ("long".equals(parameterTypeName)) {
                        args[i] = needSetParameter ? new Long(parametersStr[i]) : 0;
                    } else if (parameterTypeName.contains("Long")) {
                        args[i] = needSetParameter ? new Double(parametersStr[i]) : null;
                    } else if ("java.lang.String".equals(parameterTypeName)) {
                        args[i] = needSetParameter ? parametersStr[i] : null;
                    } else if ("java.util.Date".equals(parameterTypeName)) {
                        args[i] = needSetParameter ? new Date(Long.parseLong(parametersStr[i])) : null;
                    }
                }
            } catch (Exception e) {
                setMsg("参数解析异常");
                return;
            }
        }

    }


}
