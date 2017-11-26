package com.secret.spider.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLConnection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.htmlcleaner.TagNode;
import org.htmlcleaner.XPatherException;

public class URLConnUtils {
	public static String split = "ppp;;;";
	public static String folderSplit = ":";
	
	static Logger logger = Logger.getLogger(URLConnUtils.class);
	
	/** 
     * 向指定URL发送GET方法的请求 
     *  
     * @param url 
     *            发送请求的URL 
     * @param param 
     *            请求参数，请求参数应该是name1=value1&name2=value2的形式。 
     * @return URL所代表远程资源的响应 
     */  
    public static String sendGet(String url) {  
    	logger.info("start get: " + url);
        String result = "";  
        BufferedReader in = null;  
        try {  
            String urlName = url;  
            URL realUrl = new URL(urlName);  
            // 打开和URL之间的连接  
            URLConnection conn = realUrl.openConnection();  
            
            conn.setConnectTimeout(60000);
            conn.setReadTimeout(60000);
            conn.setAllowUserInteraction(false);         
            conn.setDoOutput(true);

            // 设置通用的请求属性  
            conn.setRequestProperty("accept", "*/*");  
            conn.setRequestProperty("connection", "Keep-Alive");  
            conn.setRequestProperty("user-agent",  
                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)");  
            // 建立实际的连接  
            conn.connect();  
            // 定义BufferedReader输入流来读取URL的响应  
            in = new BufferedReader(  
                    new InputStreamReader(conn.getInputStream(), "GBK"));  
            String line;  
            while ((line = in.readLine()) != null) {  
                result += "/n" + line;  
            }  
        } catch (Exception e) {  
            System.out.println("发送GET请求出现异常！" + url);  
            e.printStackTrace();  
        }  
        // 使用finally块来关闭输入流  
        finally {  
            try {  
                if (in != null) {  
                    in.close();  
                }  
            } catch (IOException ex) {  
                ex.printStackTrace();  
            }  
        }  
        return result;  
    }
    
    public static String getNodeText(Object[] node) {
    	String text = null;
    	
    	if(node!=null && node.length>0) {
    		try{
    			TagNode n = (TagNode) node[0];
    			text = n.getText().toString().trim();
    		} catch(Exception e) {
    			e.printStackTrace();
    		}
    	}
    	
    	return text;
    }
    
    public static String getNodeText(Object node) {
    	String text = null;
    	
    	if(node!=null) {
    		try{
    			TagNode n = (TagNode) node;
    			text = n.getText().toString().trim();
    		} catch(Exception e) {
    			e.printStackTrace();
    		}
    	}
    	
    	return text;
    }
    
    public static String getNodeTextByXpath(TagNode pNode, String xpath) throws XPatherException {
    	String text = null;
    	
    	Object[] objs = pNode.evaluateXPath(xpath);
    	
    	if(objs!=null && objs.length>0) {
    		try{
    			TagNode n = (TagNode) objs[0];
    			text = n.getText().toString().trim();
    		} catch(Exception e) {
    			e.printStackTrace();
    		}
    	}
    	
    	return text;
    }
    
    public static String getNodeTextAndAttr(Object[] node, String attr) {
    	String text = null;
    	
    	if(node!=null && node.length>0) {
    		try{
    			TagNode n = (TagNode) node[0];
    			text = n.getText().toString().trim() + ";" + n.getAttributeByName(attr);
    		} catch(Exception e) {
    			e.printStackTrace();
    		}
    	}
    	
    	return text;
    }
    
    public static String getNodeTextAndAttr(Object node, String attr) {
    	String text = null;
    	
    	if(node!=null) {
    		try{
    			TagNode n = (TagNode) node;
    			text = n.getText().toString().trim() + ";" + n.getAttributeByName(attr);
    		} catch(Exception e) {
    			e.printStackTrace();
    		}
    	}
    	
    	return text;
    }
    
    public static String getNodeAttr(Object[] node, String attr) {
    	String text = null;
    	
    	if(node!=null && node.length>0) {
    		try{
    			TagNode n = (TagNode) node[0];
    			text = n.getAttributeByName(attr);
    		} catch(Exception e) {
    			e.printStackTrace();
    		}
    	}
    	
    	return text;
    }
    
    public static String getUnicode(String s) {
        try {
            StringBuffer out = new StringBuffer("");
            byte[] bytes = s.getBytes("unicode");
            for (int i = 0; i < bytes.length - 1; i += 2) {
                out.append("\\u");
                String str = Integer.toHexString(bytes[i + 1] & 0xff);
                for (int j = str.length(); j < 2; j++) {
                    out.append("0");
                }
                String str1 = Integer.toHexString(bytes[i] & 0xff);
                out.append(str1);
                out.append(str);
                 
            }
            return out.toString();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
    }
    
    public static String unicodeToString(String str) {
        Pattern pattern = Pattern.compile("(\\\\u(\\p{XDigit}{4}))");    
        Matcher matcher = pattern.matcher(str);
        char ch;
        while (matcher.find()) {
            ch = (char) Integer.parseInt(matcher.group(2), 16);
            str = str.replace(matcher.group(1), ch + "");    
        }
        return str;
    }
    
    public static String[] getCodesByUrl(String url){
    	String[] codes = null;
    	
    	String productCode = url.substring(url.lastIndexOf("/index")+6).replace(".shtml", "").trim();
    	String bCode = productCode;
		if(bCode.length() == 6) {
			bCode = bCode.substring(0, 3);
		} else {
			bCode = bCode.substring(0, 4);
		}
		int pCode = Integer.valueOf(bCode);
		bCode = String.valueOf(pCode + 1);
		
		codes = new String[2];
		codes[0] = bCode;
		codes[1] = productCode;
    	
    	return codes;
    }
}
