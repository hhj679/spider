package com.secret.spider;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.secret.spider.util.KafkaUtils;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
//    	KafkaUtils.produceMessage("zol-bpl-request-topic", "8", "8");
    	
    	Properties props = KafkaUtils.initConsumerProps("zol-bpl-group", "false", "1");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		//订阅主题列表topic
		consumer.subscribe(Arrays.asList("zol-bpl-request-topic"));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				System.out.println(record.key() + "," + record.value());
			}
			consumer.commitSync();
		}
    }
}
