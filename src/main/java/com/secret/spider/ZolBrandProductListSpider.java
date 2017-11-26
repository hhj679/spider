package com.secret.spider;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.TagNode;

import com.secret.spider.util.KafkaUtils;
import com.secret.spider.util.URLConnUtils;

public class ZolBrandProductListSpider {
	static Logger logger = Logger.getLogger(ZolBrandProductListSpider.class);

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		//****注意：要先创建topic：zol-bpl-request-topic
//		spideBrandProductList();

		cleanProductList();
		
//		checkProductList();
		
//		checkFirstLevelProductList();
	}

	public static void spideBrandProductList() throws IOException{
		List<String> brandList = FileUtils.readLines(new File("F:\\aliyun\\zol\\data\\jiadian-brands.txt"), "UTF-8");
		for(String brand:brandList) {
			try {
				int jingIndex = brand.indexOf("#");
				if(jingIndex == 0 || jingIndex == 1) {
					continue;
				}

				String[] params = brand.split(",");
				if(params.length < 2) {
					logger.error("Split error brand product list error:" + brand);
					continue;
				}
				String[] folders = params[1].split("/");
				if(folders.length < 3) {
					logger.error("Split url error brand product list error:" + brand);
					continue;
				}

				KafkaUtils.produceMessage("zol-bpl-request-topic", 
						"product-list" + URLConnUtils.folderSplit + folders[1] + URLConnUtils.folderSplit + folders[2] + URLConnUtils.folderSplit + "1.html", 
						"http://detail.zol.com.cn" + params[1]);
			} catch (Exception e) {
				e.printStackTrace();
				logger.error(e);
				logger.error("Add request queue brand product list error:" + brand);
			}
		}
	}

	public static void cleanProductList() {
		File pFile = new File("F:\\aliyun\\zol\\data\\product-list");
		File[] cFiles = pFile.listFiles();
		for(File cFile: cFiles) {
			File[] bFiles = cFile.listFiles();
			for(File bFile:bFiles) {
				File[] prFiles = bFile.listFiles();
				for(File prFile:prFiles) {
					try{
						HtmlCleaner hc = new HtmlCleaner();
						TagNode tn = hc.clean(prFile, "UTF-8");
						Object[] brandNodes = tn.evaluateXPath("//div[@class='list-box']/div");

						File txtFile = new File(prFile.getPath().replace(".html", ".txt"));

						for(int i=0; i<brandNodes.length; i++) {
							TagNode brandNode = (TagNode) brandNodes[i];
							String className = brandNode.getAttributeByName("class");
							if(className!=null && className.contains("list-item")) {
								String name="", imgUrl = "", group = "", price = "", priceDate = "", 
										mernum = "", star = "", dp="", pc="", tz = "", params = "";

								Object[] priceNodes = brandNode.evaluateXPath("/div[@class='price-box']/span");
								if(priceNodes.length >= 1) {
									price = URLConnUtils.getNodeText(priceNodes[0]);
								}
								if(priceNodes.length >= 2) {
									priceDate = URLConnUtils.getNodeText(priceNodes[1]);
								}

								Object[] mernumNodes = brandNode.evaluateXPath("/div[@class='price-box']/p[@class='mernum']/a");
								mernum = URLConnUtils.getNodeTextAndAttr(mernumNodes, "href");

								Object[] imgNodes = brandNode.evaluateXPath("/div[@class='pic-box SP']/a/img");
								imgUrl = URLConnUtils.getNodeAttr(imgNodes, "src");
								Object[] nameNodes = brandNode.evaluateXPath("/div[@class='pro-intro']/h3/a");
								name = URLConnUtils.getNodeTextAndAttr(nameNodes, "href");

								Object[] paramsLis = brandNode.evaluateXPath("/div[@class='pro-intro']/ul/li");
								for(int j=0; j<paramsLis.length; j++) {
									TagNode n = (TagNode) paramsLis[j];
									String cn = n.getAttributeByName("class");
									if(cn != null && cn.equals("group")) {
										group = n.getText().toString().replace("/n", "").replace("&gt;", "").trim();
									} else {
										params += URLConnUtils.split + URLConnUtils.getNodeText(paramsLis[j]).replace("/n", "").replace("&gt;", "").trim();
									}
								}

								Object[] starNodes = brandNode.evaluateXPath("/div[@class='pro-intro']/div/div[@class='grade']/b");
								star = URLConnUtils.getNodeText(starNodes);
								Object[] dpNodes = brandNode.evaluateXPath("/div[@class='pro-intro']/div/div[@class='grade']/span/a");
								dp = URLConnUtils.getNodeTextAndAttr(dpNodes, "href");
								Object[] pcNodes = brandNode.evaluateXPath("/div[@class='pro-intro']/div/div[@class='links']/a");
								for(int k=0; k<pcNodes.length; k++) {
									pc += URLConnUtils.split + URLConnUtils.getNodeTextAndAttr(pcNodes[k], "href");
								}

								FileUtils.writeStringToFile(txtFile, name + URLConnUtils.split + imgUrl + URLConnUtils.split + URLConnUtils.split + group + URLConnUtils.split + price + URLConnUtils.split 
										+ priceDate + URLConnUtils.split + mernum + URLConnUtils.split + star + URLConnUtils.split + dp + URLConnUtils.split + pc + URLConnUtils.split + tz + URLConnUtils.split + params + "\r\n", "UTF-8", true);
							}

						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	public static void checkFirstLevelProductList() throws IOException {
		List<String> brandList = FileUtils.readLines(new File("F:\\aliyun\\zol\\data\\jiadian-brands.txt"), "UTF-8");
		for(String brand:brandList) {
			try {
				int jingIndex = brand.indexOf("#");
				if(jingIndex == 0 || jingIndex == 1) {
					continue;
				}
				String[] params = brand.split(",");
				if(params.length < 2) {
					logger.error("Split error brand product list error:" + brand);
					continue;
				}
				String[] folders = params[1].split("/");
				if(folders.length < 3) {
					logger.error("Split url error brand product list error:" + brand);
					continue;
				}
				
				File categoryFile = new File("F:\\aliyun\\zol\\data\\product-list\\" +folders[1] + "\\" + folders[2]);
				
				if(!categoryFile.exists()) {
					logger.error("Check Productlist, not found brand file error:" + folders[1] + "\\" + folders[2]);
					KafkaUtils.produceMessage("zol-bpl-request-topic", 
							"product-list" + URLConnUtils.folderSplit + folders[1] + URLConnUtils.folderSplit + folders[2] + URLConnUtils.folderSplit + "1.html", 
							"http://detail.zol.com.cn" + params[1]);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void checkProductList() {
		File pFile = new File("F:\\aliyun\\zol\\data\\product-list");
		File[] cFiles = pFile.listFiles();
		for(File cFile: cFiles) {
			File[] bFiles = cFile.listFiles();
			for(File bFile:bFiles) {
				logger.debug("Checking folder: " + cFile.getName() + "\\" + bFile.getName());
				File firstFile = new File("F:\\aliyun\\zol\\data\\product-list\\" + cFile.getName() + "\\" + bFile.getName() + "\\" + "1.html");
				if(firstFile.exists()) {
					try{
						HtmlCleaner hc = new HtmlCleaner();
						TagNode tn = hc.clean(firstFile, "UTF-8");
						
						int pageCount = 0;
						Object[] pageNodes = tn.evaluateXPath("//div[@class='small-page']/span[@class='small-page-active']");
						String tpStr = URLConnUtils.getNodeText(pageNodes);
						if(tpStr != null) {
							String[] tpStrs = tpStr.split("/");
							pageCount = Integer.valueOf(tpStrs[1].trim());
						}
						for(int i=2; i<= pageCount; i++) {
							File moreFile = new File("F:\\aliyun\\zol\\data\\product-list\\" + cFile.getName() + "\\" + bFile.getName() + "\\" + i + ".html");
							if(!moreFile.exists()) {
								logger.error("Check Productlist, not found more file error:" + cFile.getName() + "\\" + bFile.getName() + "\\" + i + ".html");
								KafkaUtils.produceMessage("zol-bpl-request-topic", 
										"product-list" + URLConnUtils.folderSplit + cFile.getName() + URLConnUtils.folderSplit + bFile.getName() + URLConnUtils.folderSplit + i +".html", 
										"http://detail.zol.com.cn/" + cFile.getName() + "/" + bFile.getName() + "/" + i + ".html");
							}
						}
					} catch(Exception e) {
						e.printStackTrace();
					}
				} else {
					logger.error("Check product list, not found first file error:" + cFile.getName() + "\\" + bFile.getName());
					KafkaUtils.produceMessage("zol-bpl-request-topic", 
							"product-list" + URLConnUtils.folderSplit + cFile.getName() + URLConnUtils.folderSplit + bFile.getName() + URLConnUtils.folderSplit + "1.html", 
							"http://detail.zol.com.cn/" + cFile.getName() + "/" + bFile.getName() + "/");
				}
			}
		}
	}
}