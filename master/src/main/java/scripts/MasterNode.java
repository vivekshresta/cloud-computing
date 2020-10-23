package scripts;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan({"http.service","http.filter","helper"})
public class MasterNode {

    public static void main(String[] args) {
        SpringApplication.run(MasterNode.class, args);
    }
}
