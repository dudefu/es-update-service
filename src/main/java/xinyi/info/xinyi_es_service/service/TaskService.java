package xinyi.info.xinyi_es_service.service;

import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import xinyi.info.xinyi_es_service.utils.DateUtils;

import java.util.Date;
import java.util.concurrent.Future;

/**
 * @author ：dudefu
 * @date ：Created in 2021/7/28 15:35
 * @description：
 * @version: $
 */
@Service
public class TaskService {

    @Async
    /**
     * 表明是异步调用
     * 没有返回值
     */
    public void excutVoidTask(int i) {
        System.out.println("异步执行任务第[" + i + "] 个");
    }

    /**
     * 有返回值
     * 异常调用
     *
     * @param i
     * @return
     * @throws InterruptedException
     */
    @Async
    public Future<String> excuteValueTask(int i) throws InterruptedException {
        Thread.sleep(1000);
        Future<String> future = new AsyncResult<String>("success is " + i);
        System.out.println("异步执行任务第[" + i + "] 个");
        return future;
    }

    @Async
    /**
     * 表明是异步调用
     * 没有返回值
     * ES单个index批量更新操作
     */
    public void excuteVoidUpdateESTask(String index,String type,String rangeQueryName,String updateField, String updateValue,String oldValue,
                              Date startTime, Date endTime) {
        ESService.update(index, type, rangeQueryName, updateField, updateValue,oldValue, startTime, endTime);
    }

    /**
     * 有返回值
     * @param index
     * @param type
     * @param rangeQueryName
     * @param updateField
     * @param updateValue
     * @param oldValue
     * @param startDate
     * @param endDate
     * @return
     * @throws InterruptedException
     */
    @Async
    public Future<String> excuteUpdateESTask(String index, String type, String rangeQueryName, String updateField, String updateValue, String oldValue,
                                             Date startDate, Date endDate,Date stopTime,String timeOffSetType,int offSet) throws InterruptedException {
        Thread.sleep(1000);
        System.out.println("正在更新索引[" + index + "]");
        while (endDate.getTime() >= stopTime.getTime()) {
            ESService.update(index, type, rangeQueryName, updateField, updateValue,oldValue, startDate, endDate);
            if("hour".equals(timeOffSetType)){
                startDate = DateUtils.hourOffset(startDate,offSet);
                endDate = DateUtils.hourOffset(endDate,offSet);
            }
            if("day".equals(timeOffSetType)){
                startDate = DateUtils.dayOffset(startDate,offSet);
                endDate = DateUtils.dayOffset(endDate,offSet);
            }
            if("month".equals(timeOffSetType)){
                startDate = DateUtils.monthOffset(startDate,offSet);
                endDate = DateUtils.monthOffset(endDate,offSet);
            }
        }
        Future<String> future = new AsyncResult<String>(index + " is success ");
        return future;
    }

}
