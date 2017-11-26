package com.secret.spider;

import java.io.File;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.TagNode;

import com.secret.spider.util.KafkaUtils;
import com.secret.spider.util.URLConnUtils;

public class ConsumerProductParamRequest {
	static Logger logger = Logger.getLogger(ConsumerProductParamRequest.class);
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		request("zol-product-param-request-group", "zol-product-param-request-topic");
	}
	
	public static void request(String group, String topic) {
		Properties props = KafkaUtils.initConsumerProps(group, "false", "1");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		//订阅主题列表topic
        consumer.subscribe(Arrays.asList(topic));
        
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            logger.debug("Kafka record count:" +records.count());
            for (ConsumerRecord<String, String> record : records) {
            	try {
            		String html = URLConnUtils.sendGet(record.value());
            		String[] folders = record.key().split(URLConnUtils.folderSplit); 
            		String filePath = "F:\\aliyun\\zol\\data";
            		for(String folder:folders) {
            			filePath += "\\" + folder;
            		}
            		
            		File htmlFile = new File(filePath);//不重复爬取
            		if(htmlFile.exists()){
            			continue;
            		}
            		
            		//验证是否被墙
            		HtmlCleaner hc = new HtmlCleaner();
            		TagNode tn = hc.clean(html);
            		Object[] titleNode = tn.evaluateXPath("//*[@id='page-title']");
            		if(titleNode.length > 0) {
            			FileUtils.writeStringToFile(new File(filePath), html, "UTF-8");
            			consumer.commitSync();//手动提交
            		} else {
            			logger.error("request url error: " + record.value());
            		}
            		
            		Thread.sleep(10000); //隔10s发送一次请求
            	} catch (Exception e) {
            		e.printStackTrace();
            		logger.error(e);
            		logger.error("request url error: " + record.value());
            	}
            }
        }
	}
}
