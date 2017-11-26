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

public class ConsumerBrandProudctListRequest {
	static Logger logger = Logger.getLogger(ConsumerBrandProudctListRequest.class);

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		request("zol-bpl-group", "zol-bpl-request-topic");
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

					//验证是否被墙
					HtmlCleaner hc = new HtmlCleaner();
					TagNode tn = hc.clean(html);
					Object[] titleNode = tn.evaluateXPath("//*[@id='J_CityArea']/h1");
					if(titleNode.length > 0) {
						FileUtils.writeStringToFile(new File(filePath), html, "UTF-8");
						//如果是第一页的话，负责生成后面几页的request url
						if(record.key().endsWith(URLConnUtils.folderSplit + "1.html")) {
							int pageCount = 0;
							Object[] pageNodes = tn.evaluateXPath("//div[@class='small-page']/span[@class='small-page-active']");
							String tpStr = URLConnUtils.getNodeText(pageNodes);
							if(tpStr != null) {
								String[] tpStrs = tpStr.split("/");
								pageCount = Integer.valueOf(tpStrs[1].trim());
							}

							for(int i=2; i<=pageCount; i++) {
								KafkaUtils.produceMessage("zol-bpl-request-topic", 
										"product-list" + URLConnUtils.folderSplit + folders[1] + URLConnUtils.folderSplit + folders[2] + URLConnUtils.folderSplit + i + ".html", 
										record.value() + i + ".html");
							}
						}

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
