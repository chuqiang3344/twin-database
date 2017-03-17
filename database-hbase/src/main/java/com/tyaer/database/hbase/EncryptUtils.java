package com.tyaer.database.hbase;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class EncryptUtils {
	
	public static byte[] getMD5(String url)
	{
		
		try {
			MessageDigest md;
			md = MessageDigest.getInstance("MD5");
			md.update(url.getBytes());
			byte[] key = md.digest();
			
			return key;
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static void main(String[] args) {
		String url="http://iwan.qq.com/kf/gamestart?id=204067&from=90204067&gameid=3496&url=http%3A%2F%2Fp.tcl37.net%2Fs%2F1%2F1359%2F21825.html%3Fuid%3Dqq&oid=4";
		System.out.println(new String(EncryptUtils.getMD5(url)));
	}
}
