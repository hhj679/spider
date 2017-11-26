package com.secret.spider;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.TagNode;
import org.htmlcleaner.XPatherException;

import com.secret.spider.util.URLConnUtils;

public class ZolBrandSpider {
	
	static Logger logger = Logger.getLogger(ZolBrandSpider.class);

	public static void main(String[] args) throws XPatherException, IOException, InterruptedException {
		// TODO Auto-generated method stub
//		spideCategory("F:\\aliyun\\zol\\data\\subcategory.txt"); //step 1
		
		
		//step 2
//		File file = new File("F:\\aliyun\\zol\\data\\jiadian-brands.txt");
//		List<String> lines = FileUtils.readLines(new File("F:\\aliyun\\zol\\data\\jiadian-category.txt"), "UTF-8");
//		for(String line:lines) {
//			if(line.startsWith("#")){
//				continue;
//			}
//			spideCategoryBrands(line, file);
//			Thread.sleep(10000);
//		}
		
		//step 3
		int[] sleepTimes = {9000, 7000, 8000, 10000, 11000};
		List<String> lines = FileUtils.readLines(new File("F:\\aliyun\\zol\\data\\jiadian-brands.txt"), "UTF-8");
		for(String line:lines) {
			if(line.startsWith("#")){
				continue;
			}
			int page = 0;
			try {
				page = spideBrandProducts(line, 1);
			} catch(Exception e) {
				e.printStackTrace();
//				System.out.println("error: " + line);
				logger.error("error: " + line);
			}
			Thread.sleep(10000);
			for(int i=2; i<=page; i++) {
				try {
					spideBrandProducts(line, i);
					int num =(int)(Math.random() * 5);
					Thread.sleep(sleepTimes[num]);
				} catch(Exception e1) {
					e1.printStackTrace();
//					System.out.println("error: " + line + "," + i);
					logger.error("error at: " + line + "," + i);
				}
			}
		}
	}
	
	public static void spideCategory(String savePath) throws XPatherException, IOException {
		String html = URLConnUtils.sendGet("http://detail.zol.com.cn/subcategory.html");
		HtmlCleaner hc = new HtmlCleaner();
		TagNode tn = hc.clean(html);
		Object[] lis = tn.evaluateXPath("//div[@class='mod-cate-box']/ul/li");
		File file = new File(savePath);
		for(int i=0; i<lis.length; i++) {
			TagNode li = (TagNode) lis[i];
			TagNode categoryNode = (TagNode) li.evaluateXPath("/h3/a")[0];
			if(categoryNode != null) {
				FileUtils.writeStringToFile(file, "#" + categoryNode.getText().toString().trim() + "," + categoryNode.getAttributeByName("href") + "\r\n", "UTF-8", true);
			}
			Object[] subcategoryNodes = li.evaluateXPath("/div/a");
			for(int j=0; j<subcategoryNodes.length; j++) {
				TagNode subcategoryNode = (TagNode) subcategoryNodes[j];
				FileUtils.writeStringToFile(file, subcategoryNode.getText().toString().trim() + "," + subcategoryNode.getAttributeByName("href") + "\r\n", "UTF-8", true);
			}
		}
	}

	public static void spideCategoryBrands(String categoryLine, File file) throws XPatherException, IOException{
		String[] urls = categoryLine.split(",");
		String html = URLConnUtils.sendGet("http://detail.zol.com.cn" + urls[1]);
		HtmlCleaner hc = new HtmlCleaner();
		TagNode tn = hc.clean(html);
		Object[] brandNodes = tn.evaluateXPath("//div[@id='J_ParamBrand']/a");
		FileUtils.write(file, "#" + categoryLine + "\r\n", "UTF-8", true);
		for(int i=0; i<brandNodes.length; i++) {
			TagNode brandNode = (TagNode) brandNodes[i];
			FileUtils.write(file, brandNode.getText().toString().trim() + "," + brandNode.getAttributeByName("href") + "\r\n", "UTF-8", true);
		}
	}
	
	public static int spideBrandProducts(String brandLines, int page) throws XPatherException, IOException {
		String[] urls = brandLines.split(",");
		String pageUrl = "";
		
		String[] paths = urls[1].split("/");
		String filePath = "F:\\aliyun\\zol\\data\\products" + "\\" + paths[1] + "\\" + paths[2] + "\\" + page + ".html";
		
		File htmlFile = new File(filePath);
		File txtFile = new File(filePath.replace(".html", ".txt"));
		
		if(page > 1) {
			pageUrl = page + ".html";
		}
		
		String html = URLConnUtils.sendGet("http://detail.zol.com.cn" + urls[1] + pageUrl);
		FileUtils.writeStringToFile(htmlFile, html, "UTF-8");
		
		HtmlCleaner hc = new HtmlCleaner();
		TagNode tn = hc.clean(html);
		Object[] brandNodes = tn.evaluateXPath("//div[@class='list-box']/div");
		
		int pageCount = 0;
		Object[] pageNodes = tn.evaluateXPath("//div[@class='small-page']/span[@class='small-page-active']");
		String tpStr = URLConnUtils.getNodeText(pageNodes);
		if(tpStr != null) {
			String[] tpStrs = tpStr.split("/");
			pageCount = Integer.valueOf(tpStrs[1].trim());
		}
		
		for(int i=0; i<brandNodes.length; i++) {
			TagNode brandNode = (TagNode) brandNodes[i];
			String className = brandNode.getAttributeByName("class");
			if(className!=null && className.contains("list-item")) {
				String name="", imgUrl = "", group = "", price = "", priceDate = "", 
						mernum = "", star = "", dp="", pc="", tz = "", params = "";
				
//				Object[] priceNodes = brandNode.evaluateXPath("/div[@class='price-box']/span[@class='price price-normal']");
//				price = URLConnUtils.getNodeText(priceNodes);
//				Object[] priceDateNodes = brandNode.evaluateXPath("/div[@class='price-box']/span[@class='date']");
//				priceDate = URLConnUtils.getNodeText(priceDateNodes);
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
		
		return pageCount;
	}
}
