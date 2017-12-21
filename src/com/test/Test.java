package com.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		/*try {
			BufferedReader fileBr = new BufferedReader(new FileReader("/tmp/test.pid"));
			String pid = fileBr.readLine();
			String cmd = "top -b -p " + pid + " -n 1";
			Process process = Runtime.getRuntime().exec(cmd);
			
			process.waitFor();
			
			InputStream input = process.getInputStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(input));
			
			String line = null;
			while((line=br.readLine())!=null){
				System.out.println(line);
			}
			
			fileBr.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			
		}*/
		
		Properties prop = new Properties();
		InputStream input = Test.class.getClassLoader().
				getResourceAsStream("monitor.properties");
		try {
			prop.load(input);
			System.out.println(prop.getProperty("kafka.host"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
