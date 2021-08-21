package com.xxl.job.admin.core.scheduler;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.thread.*;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.client.ExecutorBizClient;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author xuxueli 2018-10-28 00:18:17
 */

public class XxlJobScheduler  {
    private static final Logger logger = LoggerFactory.getLogger(XxlJobScheduler.class);


    public void init() throws Exception {
        // init i18n
        initI18n();

        // admin trigger pool start
        // 维护两个线程池：快慢线程池。每一次触发任务执行都会在线程池中创建一个线程。
        // 线程执行逻辑仅仅是调用XxlJobTrigger.trigger(),可见触发任务执行的耗时操作都在trigger中。
        // 快慢线程池的选择通过一分钟内触发超时次数来决定，超时阈值是500毫米，次数是10。如果一个任务在一分钟内触发超时10次，那么在第十一次触发线程将会使用慢线程池执行。
        JobTriggerPoolHelper.toStart();

        // admin registry monitor run
        // 主要是维护组内机器的心跳（表：xxl-job-registry）以及组的机器地址（xxl-job-group）
        // 一个组指的是一个服务集群，如果一个服务部署在多台机器，那么这多个机器就是一个组。
        // 部署服务的机器会定时上报心跳存入xxl-job-registry表中，如果集群服务的某台机器超过90（3个心跳的时间）秒未上报心跳，则视其死亡，会从xxl-job-registry中删除该条记录
        // 仅在心跳表中删除还不够，还需在组表中，将该机器的ip删除，这样在路由的时候才不会触发执行任务的请求分发到死亡的机器上。
        // JobRegistryHelper内除启动了一个线程池和一个监控线程
        // 线程池用来生成线程完成心跳上报的逻辑（心跳上报逻辑简单但频繁，所以启用线程），监控线程则每30秒（一个心跳）扫一次心跳表，将死亡的机器删除，将对应组中死亡机器的ip删除。
        JobRegistryHelper.getInstance().start();

        // admin fail-monitor run
        // 主要是任务执行失败的重试、报警
        // 每次任务执行，都会在表中存一条调度记录，里面记录了任务执行成功与否。
        // JobFailMonitorHelper里面启动了一个线程，该线程每10秒执行一次，逻辑是查询所有执行失败的执行记录，针对这些记录依次执行一下逻辑
        // 1.设置成锁定状态，设置失败则不执行后续逻辑
        // 2.当前执行失败次数和设置重试次数进行比较，依次决定是否重新触发一次任务
        // 3.通过邮件报警，根据报警结果，设置本此执行的调度记录报警状态（报警成功、报警失败、无需报警）。报警的细节是：拿到所有JobAlarm类型的bean，执行其doAlarm方法，所以如果要自定义报警，只需实现接口JobAlarm
        JobFailMonitorHelper.getInstance().start();

        // admin lose-monitor run ( depend on JobTriggerPoolHelper )
        // 内部启动一个回调线程池以及监控线程
        // 监控线程一分钟执行一次，将处于运行中状态超过十分钟&&执行任务的机器不在线（停跳没了）的调度记录直接设置为失败更新到xxl-job-log表中表中。
        // 回调线程后续再整理
        JobCompleteHelper.getInstance().start();

        // admin log report start
        // 每隔一分钟统计最近三天任务执行总数、成功次数、失败次数。
        // 每天执行一次清除log的操作，保留天数可自定义，默认为7天，如果设置值比7小则表示关闭清除功能。
        JobLogReportHelper.getInstance().start();

        // start-schedule  ( depend on JobTriggerPoolHelper )
        // 扫描任务，触发到时间的任务，配置了fire_once_more的任务，延期后依然会触发一次。更新任务的下次触发时间。
        JobScheduleHelper.getInstance().start();

        logger.info(">>>>>>>>> init xxl-job admin success.");
    }

    
    public void destroy() throws Exception {

        // stop-schedule
        JobScheduleHelper.getInstance().toStop();

        // admin log report stop
        JobLogReportHelper.getInstance().toStop();

        // admin lose-monitor stop
        JobCompleteHelper.getInstance().toStop();

        // admin fail-monitor stop
        JobFailMonitorHelper.getInstance().toStop();

        // admin registry stop
        JobRegistryHelper.getInstance().toStop();

        // admin trigger pool stop
        JobTriggerPoolHelper.toStop();

    }

    // ---------------------- I18n ----------------------

    private void initI18n(){
        for (ExecutorBlockStrategyEnum item:ExecutorBlockStrategyEnum.values()) {
            item.setTitle(I18nUtil.getString("jobconf_block_".concat(item.name())));
        }
    }

    // ---------------------- executor-client ----------------------
    private static ConcurrentMap<String, ExecutorBiz> executorBizRepository = new ConcurrentHashMap<String, ExecutorBiz>();
    public static ExecutorBiz getExecutorBiz(String address) throws Exception {
        // valid
        if (address==null || address.trim().length()==0) {
            return null;
        }

        // load-cache
        address = address.trim();
        ExecutorBiz executorBiz = executorBizRepository.get(address);
        if (executorBiz != null) {
            return executorBiz;
        }

        // set-cache
        executorBiz = new ExecutorBizClient(address, XxlJobAdminConfig.getAdminConfig().getAccessToken());

        executorBizRepository.put(address, executorBiz);
        return executorBiz;
    }

}
