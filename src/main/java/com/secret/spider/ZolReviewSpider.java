package com.secret.spider;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.TagNode;
import org.htmlcleaner.XPatherException;

import com.secret.spider.util.KafkaUtils;
import com.secret.spider.util.URLConnUtils;

public class ZolReviewSpider {
	static Logger logger = Logger.getLogger(ZolReviewSpider.class);
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		//create topic first
		//kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic zol-product-review-request-topic
//		spideProductReviewHome();
		
		
		cleanHomeHtml();
	}
	
	public static void spideProductReviewHome() {
		File pFile = new File("F:\\aliyun\\zol\\data\\products");
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
									File htmlFile = new File("F:\\aliyun\\zol\\data\\review\\home\\" + cFile.getName() + "\\" + bFile.getName() + codes[1] + ".html");
									if(htmlFile.exists()) {
										continue;
									}
									KafkaUtils.produceMessage("zol-product-review-request-topic", 
											"review" + URLConnUtils.folderSplit + "home" + URLConnUtils.folderSplit + cFile.getName() + URLConnUtils.folderSplit + bFile.getName() + URLConnUtils.folderSplit + codes[1] + ".html", 
											"http://detail.zol.com.cn/" + codes[0] + "/" + codes[1] + "/review.shtml");
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
	
	public static void cleanHomeHtml() {
		File hFile = new File("F:\\aliyun\\zol\\data\\review\\home");
		File sFile = new File("F:\\aliyun\\zol\\data\\review\\review-summary.txt");
		File[] hFiles = hFile.listFiles();
		for(File cFile:hFiles) {
			File[] cFiles = cFile.listFiles();
			for(File bFile: cFiles) {
				File[] rFiles = bFile.listFiles();
				for(File rFile:rFiles) {
					try {
						HtmlCleaner hc = new HtmlCleaner();
						TagNode tn = hc.clean(rFile, "UTF-8");
						String totalScore = URLConnUtils.getNodeTextByXpath(tn, "//div[@class='total-score']/strong");
						if(totalScore == null || totalScore.trim().length() <=0) {
							continue;
						}
						String totalScoreum = URLConnUtils.getNodeTextByXpath(tn, "//div[@class='total-score']/div[@class='total-num']/span");
						String totalReview = URLConnUtils.getNodeTextByXpath(tn, "//div[@id='tagNav']/ul[@class='nav']/li[@class='active']/span/em");
						String filter1 = URLConnUtils.getNodeTextByXpath(tn, "//div[@id='J_CommentFilter']/div/label[2]/em");
						String filter2 = URLConnUtils.getNodeTextByXpath(tn, "//div[@id='J_CommentFilter']/div/label[3]/em");
						String filter3 = URLConnUtils.getNodeTextByXpath(tn, "//div[@id='J_CommentFilter']/div/label[4]/em");
						String filter4 = URLConnUtils.getNodeTextByXpath(tn, "//div[@id='J_CommentFilter']/div/label[5]/em");
						String filter5 = URLConnUtils.getNodeTextByXpath(tn, "//div[@id='J_CommentFilter']/div/label[6]/em");
						String filter6 = URLConnUtils.getNodeTextByXpath(tn, "//div[@id='J_CommentFilter']/div/label[7]/em");
						String features = "";
						Object[] flis = tn.evaluateXPath("//div[@class='features-score']/ul/li");
						for(int i=0; i<flis.length; i++) {
							TagNode li = (TagNode) flis[i];
							String fName = URLConnUtils.getNodeTextByXpath(li, "//div[@class='name']");
							if(fName != null) {
								fName = fName.replace("/n", "").trim();
							}
							String fValue = URLConnUtils.getNodeTextByXpath(li, "//div[@class='bar']");
							if(fValue != null) {
								fValue = fValue.replace("/n", "").trim();
							}
							features += fName + ":" + fValue + ";";
						}
						FileUtils.writeStringToFile(sFile, 
								rFile.getName().replace(".html", "") + URLConnUtils.split + totalScore + URLConnUtils.split + totalScoreum + URLConnUtils.split +
								totalReview + URLConnUtils.split + filter1 + URLConnUtils.split + filter2 + URLConnUtils.split + filter3 + URLConnUtils.split + 
								filter4 + URLConnUtils.split + filter5 + URLConnUtils.split + filter6 + URLConnUtils.split + features + "\r\n", 
								"UTF-8", true);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	public static void spideProductReviewPage() {
		try {
			List<String> rsLines = FileUtils.readLines(new File("F:\\aliyun\\zol\\data\\review\\review-summary.txt"), "UTF-8");
			for(String rsLine:rsLines) {
				try {
					String[] params = rsLine.split(URLConnUtils.split);
					String reviewCount = params[3].replace("(", "").replace(")", "").trim();
					int rc = Integer.valueOf(reviewCount);
					int pc = ((int)rc/10) + 1;
					for(int i=1; i<=pc; i++) {
						KafkaUtils.produceMessage("zol-prp-request-topic", 
								"review" + URLConnUtils.folderSplit + "pages" + URLConnUtils.folderSplit + params[0] + "_" + i + ".json", 
								"http://detail.zol.com.cn/xhr3_Review_GetListAndPage_isFilter=1%5EproId=" + params[0] + "%5Epage=" + i + ".html");
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void spideProductReview() {
		File pFile = new File("F:\\aliyun\\zol\\data\\products");
		File[] cFiles = pFile.listFiles();
		for(File cFile: cFiles) {
			File[] bFiles = cFile.listFiles();
			for(File bFile:bFiles) {
				for(File prFile: bFiles) {
					if(prFile.getName().endsWith(".html")){
						continue;
					}

					try {
						List<String> productLines = FileUtils.readLines(prFile, "UTF-8");
						for(String productLine:productLines) {
							try {
								String[] pParams = productLine.split(URLConnUtils.split);
								String[] tmpStrs = pParams[0].split(";");
								String productCode = tmpStrs[0].substring(tmpStrs[0].lastIndexOf("/index")+6).replace(".shtml", "").trim();

								String bCode = "0";
								if(productCode.length() == 6) {
									productCode = productCode.substring(0, 3);
								} else {
									productCode = productCode.substring(0, 4);
								}
								int pCode = Integer.valueOf(productCode);
								bCode = String.valueOf(pCode + 1);

								int reviewCount = spideReviewHome("http://detail.zol.com.cn/" + bCode + "/" + pCode + "/review.shtml", 
										"F:\\aliyun\\zol\\data\\review\\" + cFile.getName() + "\\" + bFile.getName() + "\\", pCode);
								for(int i=1; i<=reviewCount; i++) {
//									spideReviewOnePage()
								}

							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
			}
		}
	}
	
	public static int spideReviewHome(String url, String filePath, int pCode) throws XPatherException, IOException, InterruptedException {
		int count = 0;
		String html = URLConnUtils.sendGet(url);
		FileUtils.writeStringToFile(new File(filePath + "review_home_" + pCode + ".html"), html, "UTF-8");
		HtmlCleaner hc = new HtmlCleaner();
		TagNode tn = hc.clean(html);
		Object[] lis = tn.evaluateXPath("//div[@id='J_CommentList']");
		if(lis != null && lis.length > 0) {
			String reviewCount = URLConnUtils.getNodeTextByXpath(tn, "//div[@id='tagNav']/ul[@class='nav']/li[@class='active']/span/em");
			if(reviewCount != null) {
				reviewCount = reviewCount.replace("(", "").replace(")", "").trim();
				count = Integer.valueOf(reviewCount);
			}
			
			String reviewRate = URLConnUtils.getNodeTextByXpath(tn, "//div[@class='total-score']/strong");
			String reviewCatogory1 = URLConnUtils.getNodeTextByXpath(tn, "//div[@id='J_CommentFilter']/div/lable[2]/em");
			reviewCatogory1 = reviewCatogory1.replace("(", "").replace(")", "").trim();
			String reviewCatogory2 = URLConnUtils.getNodeTextByXpath(tn, "//div[@id='J_CommentFilter']/div/lable[3]/em");
			reviewCatogory2 = reviewCatogory2.replace("(", "").replace(")", "").trim();
			String reviewCatogory3 = URLConnUtils.getNodeTextByXpath(tn, "//div[@id='J_CommentFilter']/div/lable[4]/em");
			reviewCatogory3 = reviewCatogory3.replace("(", "").replace(")", "").trim();
			String reviewCatogory4 = URLConnUtils.getNodeTextByXpath(tn, "//div[@id='J_CommentFilter']/div/lable[5]/em");
			reviewCatogory4 = reviewCatogory4.replace("(", "").replace(")", "").trim();
			String reviewCatogory5 = URLConnUtils.getNodeTextByXpath(tn, "//div[@id='J_CommentFilter']/div/lable[6]/em");
			reviewCatogory5 = reviewCatogory5.replace("(", "").replace(")", "").trim();
			String reviewCatogory6 = URLConnUtils.getNodeTextByXpath(tn, "//div[@id='J_CommentFilter']/div/lable[7]/em");
			reviewCatogory6 = reviewCatogory6.replace("(", "").replace(")", "").trim();
			
			String reviewFeatures = "";
			Object[] fObjs = tn.evaluateXPath("//div[@class='features-score']/ul/li");
			for(int i=0; i<fObjs.length; i++) {
				TagNode fn = (TagNode) fObjs[i];
				TagNode[] fns = fn.getElementsByName("div", false);
				if(fns.length >= 2) {
					reviewFeatures += URLConnUtils.getNodeText(fns[1]) + "," + URLConnUtils.getNodeText(fns[0]) + ";";
				}
			}
			
			FileUtils.writeStringToFile(new File(filePath + "review_home_" + pCode + ".txt"),
					reviewCount + URLConnUtils.split + reviewRate + URLConnUtils.split + reviewCatogory1 + 
					URLConnUtils.split + reviewCatogory2 + URLConnUtils.split + reviewCatogory3 +
					URLConnUtils.split + reviewCatogory4 + URLConnUtils.split + reviewCatogory5 +
					URLConnUtils.split + reviewCatogory6 + URLConnUtils.split + reviewFeatures, "UTF-8", true);
		}
		Thread.sleep(10000);
		return count;
	}
	
	public static int spideReviewOnePage(String url, String filePath, int pCode, int page) throws XPatherException, IOException, InterruptedException {
		int count = 0;
		String html = URLConnUtils.sendGet(url);
		FileUtils.writeStringToFile(new File(filePath + "review_" + pCode + "_" + page + ".html"), html, "UTF-8");
		HtmlCleaner hc = new HtmlCleaner();
		TagNode tn = hc.clean(html);
		Object[] lis = tn.evaluateXPath("//div[@class='comment-list']/li");
		for(int j=0; j<lis.length; j++) {
			String userName = URLConnUtils.getNodeTextByXpath(tn, "//div[@class='comments-user']/div[@class='comments-user-name']/span");
			String userDate = URLConnUtils.getNodeTextByXpath(tn, "//div[@class='comments-user']/div[@class='comments-user-name']/p");
			String score = URLConnUtils.getNodeTextByXpath(tn, "//div[@class='comments-list-content']/div/span[@class='score']");//分数
			String title = URLConnUtils.getNodeTextByXpath(tn, "//div[@class='comments-list-content']/div[@class='comments-content']/h3/a");
			title = URLConnUtils.getUnicode(title);
			String detailUrl = URLConnUtils.getNodeTextByXpath(tn, "//div[@class='comments-list-content']/div[@class='comments-content']/h3/a");
			String reviewDate = URLConnUtils.getNodeTextByXpath(tn, "//div[@class='comments-list-content']/div/span[@class='date']");
			String content = URLConnUtils.getNodeTextByXpath(tn, "//div[@class='comments-list-content']/div[@class='comments-content']/div/div[@class='content-inner']");
			content = URLConnUtils.getUnicode(content);
			String isEssence = URLConnUtils.getNodeTextByXpath(tn, "//div[@class='essence-icon']");
			String repy = URLConnUtils.getNodeTextByXpath(tn, "//div[@class='comments-list-content']/div/div[@class='J_ShowRepy']");
			String help = URLConnUtils.getNodeTextByXpath(tn, "//div[@class='comments-list-content']/div/div[@class='J_ReviewHelp']");
			String unhelp = URLConnUtils.getNodeTextByXpath(tn, "//div[@class='comments-list-content']/div/div[@class='J_ReviewUnhelp']");
			
			FileUtils.writeStringToFile(new File(filePath + "review_" + pCode + "_" + page + ".txt"),
					detailUrl + URLConnUtils.split + title + URLConnUtils.split + userName +
					URLConnUtils.split + userDate + URLConnUtils.split + score +
					URLConnUtils.split + reviewDate + URLConnUtils.split + isEssence +
					URLConnUtils.split + repy + URLConnUtils.split + help + URLConnUtils.split + unhelp + URLConnUtils.split + content, "UTF-8", true);
		}
		
		Thread.sleep(10000);
		
		return count;
	}
}
