package xinyi.info.xinyi_es_service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableAsync;
import xinyi.info.xinyi_es_service.task.TaskRunner;
import xinyi.info.xinyi_es_service.task.UpdateTasks;

import java.util.concurrent.ExecutionException;

/**
 * @author dudefu
 */
@SpringBootApplication
public class XinyiEsServiceApplication {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        SpringApplication.run(XinyiEsServiceApplication.class, args);
//        UpdateTasks taskExecutor = new UpdateTasks();
//        taskExecutor.executeReturn();
    }
    @Bean
    public TaskRunner taskRunner(){
        return new TaskRunner();
    }
}
