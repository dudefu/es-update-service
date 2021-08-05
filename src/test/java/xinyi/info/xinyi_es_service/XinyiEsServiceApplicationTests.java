package xinyi.info.xinyi_es_service;

import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.test.context.junit4.SpringRunner;
import xinyi.info.xinyi_es_service.service.TaskService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@RunWith(SpringRunner.class)
@SpringBootTest
class XinyiEsServiceApplicationTests {

    @Autowired
    private TaskService service;

    @Test
    void contextLoads() {
    }

    /**
     * 没有返回值测试
     */
    @Test
    public void testVoid() {
        for (int i = 0; i < 20; i++) {
            service.excutVoidTask(i);
        }
        System.out.println("========主线程执行完毕=========");
    }

    @Test
    public void testReturn() throws InterruptedException, ExecutionException {
        List<Future<String>> lstFuture = new ArrayList<>();// 存放所有的线程，用于获取结果
        for (int i = 0; i < 100; i++) {
            while (true) {
                try {
                    // 线程池超过最大线程数时，会抛出TaskRejectedException，则等待1s，直到不抛出异常为止
                    Future<String> stringFuture = service.excuteValueTask(i);
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
    }

}
