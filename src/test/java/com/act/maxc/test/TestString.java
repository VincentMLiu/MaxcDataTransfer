package com.act.maxc.test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestString {

	public static void main(String[] args) {
		
		
		Pattern pt = Pattern.compile("CDR[1-9]\\\\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])");
		
		System.out.println(pt.matches("[1-9]\\d{3}(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])", "20191015"));
		
		
		
		
		String testStr = "CDR220191015";
		Pattern p = Pattern.compile("^CDR2[1-9]\\d{3}(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])$");
		Matcher m = p.matcher(testStr);
		System.out.println(m.matches());

		
		String oldBody = "1571047380|++|50|++|13696133550|++|0833|++|05926279140|++|0592|++|028|++|104.055801|++|30.641857|++|1|++||++|63.com %%%% 请点击【 http://t.yimt.cn/nkY9Xv 】，邀请也已发送到您的邮箱【 13696133550@163.com 】，感|++||++||++|YJ_GJZ_URL";
		String originalDelimiter = "\\|\\+\\+\\|";
		String newDelimiter = "|";
		String[] oldBodySpli = oldBody.split(originalDelimiter);
	    StringBuffer sb = new StringBuffer();
	    for(String col : oldBodySpli) {
	    	sb.append(col).append(newDelimiter);
	    }

	    String newBody = sb.toString();
	    newBody = newBody.substring(0, newBody.lastIndexOf(newDelimiter));
	    

	    System.out.println(newBody);
	}

}
