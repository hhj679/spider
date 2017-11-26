package com.secret.spider;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.secret.spider.util.KafkaUtils;
import com.secret.spider.util.URLConnUtils;

public class ZolProductParamSpider {
	static Logger logger = Logger.getLogger(ZolProductParamSpider.class);
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//****注意：请线创建topic：zol-product-param-request-topic
		spideProductReviewHome();
	}
	
	public static void spideProductReviewHome() {
		File pFile = new File("F:\\aliyun\\zol\\data\\product-list");
		File[] cFiles = pFile.listFiles();
		for(File cFile: cFiles) {
			File[] bFiles = cFile.listFiles();
			for(File bFile:bFiles) {
				File[] prFiles = bFile.listFiles();
				for(File prFile: prFiles) {
					if(prFile.getName().endsWith(".html")){
						continue;
					}
					try {
						List<String> productLines = FileUtils.readLines(prFile, "UTF-8");
						for(String productLine:productLines) {
							try {
								String[] paramsStr = productLine.split(URLConnUtils.split);
								String[] codes = URLConnUtils.getCodesByUrl(paramsStr[0]);
								if(codes!=null && codes.length == 2) {
									File htmlFile = new File("F:\\aliyun\\zol\\data\\param\\" + cFile.getName() + "\\" + bFile.getName() + codes[1] + ".html");
									if(htmlFile.exists()) {
										continue;
									}
									KafkaUtils.produceMessage("zol-product-param-request-topic", 
											"param" + URLConnUtils.folderSplit + cFile.getName() + URLConnUtils.folderSplit + bFile.getName() + URLConnUtils.folderSplit + codes[1] + ".html", 
											"http://detail.zol.com.cn/" + codes[0] + "/" + codes[1] + "/param.shtml");
									Thread.sleep(500);
								} else {
									logger.error("Get codes fail at:" + cFile.getName() + "/" + bFile.getName());
								}
							} catch (Exception e) {
								e.printStackTrace();
								logger.error(e);
								logger.error("Request review error at line:" + productLine);
							}
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						logger.error(e);
						logger.error("Read file error review:" + cFile.getName() + "/" + bFile.getName());
					}
				}
			}
		}
	}
}
