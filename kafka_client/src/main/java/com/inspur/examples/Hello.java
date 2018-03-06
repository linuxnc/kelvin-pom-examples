package com.inspur.examples;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Hello {

    private static Log log = LogFactory.getLog(Hello.class);

    public static void main(String[] args) {
	// write your code here

        log.info("Test case starting..................................................");

        KafkaProperties kafkaProc = KafkaProperties.getInstance();

        kafkaProc.getValues("bootstrap.servers","0.0.0.0:9092");

        log.info("Test case shutdown..................................................");
    }
}
