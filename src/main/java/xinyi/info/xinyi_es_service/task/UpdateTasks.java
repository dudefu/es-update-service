package xinyi.info.xinyi_es_service.task;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import xinyi.info.xinyi_es_service.service.TaskService;
import xinyi.info.xinyi_es_service.utils.DateUtils;
import xinyi.info.xinyi_es_service.utils.PropertiesUtils;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author ：dudefu
 * @date ：Created in 2021/7/28 15:37
 * @description：
 * @version: $
 */
public class UpdateTasks {

    public void execute(TaskService service) {
        int updateNums = Integer.parseInt(PropertiesUtils.getString("update.nums"));
        String startTime = PropertiesUtils.getString("update.startTime");
        String endTime = PropertiesUtils.getString("update.endTime");
        Date startDate = DateUtils.parse(startTime);
        Date endDate = DateUtils.parse(endTime);
        Map<String, String> map;
        for (int i = 1; i < updateNums+1 ; i++) {
            map = setName(i);
            service.excuteVoidUpdateESTask(map.get("index"), map.get("type"), map.get("rangeQueryName"), map.get("updateField"),
                    map.get("updateValue"), map.get("oldValue"), startDate, endDate);
            service.excutVoidTask(i);
        }
        System.out.println("========主线程执行完毕=========");
        System.exit(0);
    }

    public void executeReturn(TaskService service) throws InterruptedException, ExecutionException {

        int updateNums = Integer.parseInt(PropertiesUtils.getString("update.nums"));
        String stopTime = PropertiesUtils.getString("update.stopTime");
        String startTime = PropertiesUtils.getString("update.startTime");
        String endTime = PropertiesUtils.getString("update.endTime");
        String timeOffSetType = PropertiesUtils.getString("update.time.offset.type");
        int timeOffSet = Integer.parseInt(PropertiesUtils.getString("update.time.offset"));
        Date stopDate = DateUtils.parse(stopTime);
        Date startDate = DateUtils.parse(startTime);
        Date endDate = DateUtils.parse(endTime);
        Map<String, String> map;

        // 存放所有的线程，用于获取结果
        List<Future<String>> lstFuture = new ArrayList<>();
        for (int i = 1; i < updateNums+1 ; i++) {
            while (true) {
                try {
                    map = setName(i);
                    // 线程池超过最大线程数时，会抛出TaskRejectedException，则等待1s，直到不抛出异常为止
                    Future<String> stringFuture = service.excuteUpdateESTask(map.get("index"), map.get("type"), map.get("rangeQueryName"), map.get("updateField"),
                            map.get("updateValue"), map.get("oldValue"), startDate, endDate,stopDate,timeOffSetType,timeOffSet);
                    lstFuture.add(stringFuture);
                    break;
                } catch (TaskRejectedException e) {
                    System.out.println("线程池满，等待1S。");
                    Thread.sleep(1000);
                }
            }
        }

        // 获取值.get是阻塞式，等待当前线程完成才返回值
        for (Future<String> future : lstFuture) {
            System.out.println(future.get());
        }

        System.out.println("========主线程执行完毕=========");
        System.exit(0);
    }

    public static Map<String, String> setName(int i) {
        Map<String, String> map = new HashMap<>();
        map.put("index", PropertiesUtils.getString("es." + i + ".index"));
        map.put("type", PropertiesUtils.getString("es." + i + ".type"));
        map.put("rangeQueryName", PropertiesUtils.getString("es." + i + ".rangeQueryName"));
        map.put("updateField", PropertiesUtils.getString("es." + i + ".updateField"));
        map.put("updateValue", PropertiesUtils.getString("es." + i + ".updateValue"));
        map.put("oldValue", PropertiesUtils.getString("es." + i + ".oldValue"));
        return map;
    }
}
