package com.container.monitor;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import com.kafka.client.KafkaProducerClient;

public class ContainerResourceMonitor {
	
	private static final Log LOG = LogFactory.getLog(ContainerResourceMonitor.class);
	
	private Runtime runtime;
	
	private String appId;
	private int allocatedMem;
	private Map<String, String> containerPids;
	
	private Options opts;
	
	private KafkaProducerClient<String, String> producer;
	
	private int threshold;
	
	public ContainerResourceMonitor() {
		appId = "";
		allocatedMem = 0;
		containerPids = new HashMap<String, String>();
		runtime = Runtime.getRuntime();
		
		opts = new Options();
		
		Properties prop = new Properties();
		InputStream input = ContainerResourceMonitor.class.
				getClassLoader().getResourceAsStream("monitor.properties");
		try {
			prop.load(input);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error("read properties file error");
		}
		
		String host = prop.getProperty("kafka.host");
		String port = prop.getProperty("kafka.port");
		String topic = prop.getProperty("container.warning.topic");
		producer = new KafkaProducerClient<String, String>(host + ":" + port);
		producer.addTopic(topic);
		
		threshold = Integer.parseInt(prop.getProperty("threshold"));
	}
	
	public void init(String[] args) {
		
		opts.addOption("app_id", true, "app id of the containers");
		opts.addOption("allocated_mem", true, "allocated memory of each container");
		
		CommandLine cliParser = null;
		try {
			cliParser = new GnuParser().parse(opts, args);
			
			appId = cliParser.getOptionValue("app_id", "");
			LOG.info("get app id:" + appId);
			
			allocatedMem = Integer.parseInt(cliParser.getOptionValue("allocated_mem", "0"));
			LOG.info("get allocated memory:" + allocatedMem);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			LOG.error("parse command args error", e);
		}
		
	}
	
	public void monitor(String containerId) {
		
		try {
			String path = "/tmp/hadoop-root/nm-local-dir/nmPrivate/" + 
					appId + "/" + containerId + "/" + containerId + ".pid";
			LOG.info("read container pid from file:" + path);
			
			BufferedReader pidBr = new BufferedReader(new FileReader(path));
			String pid = pidBr.readLine();
			LOG.info("pid of container " + containerId + " is " + pid);
			
			containerPids.put(pid, containerId);
			
			while(true){
				String cmd = "top -b -p " + pid + " -n 1";
				Process proc = runtime.exec(cmd);
				
				proc.waitFor();
				InputStream input = proc.getInputStream();
				BufferedReader procBr = new BufferedReader(new InputStreamReader(input));
				
				String line = null;
				String info = null;
				while((line=procBr.readLine())!=null){
					LOG.info(line);
					info = line;
				}
				
				String[] infoArray = info.split(" ");
				if(!infoArray[0].equals(pid)){
					break;
				}
				int memUsage = Integer.parseInt(infoArray[5]);
				int memPercent = Integer.parseInt(infoArray[9]);
				LOG.info("the memory used by container " + containerId + 
						" with pid " + pid + " is " + memUsage + " -- " + memPercent);
				
				if(memPercent>threshold){
					JSONObject msgJson = new JSONObject();
					msgJson.put("container_id", containerId);
					msgJson.put("mem_usage", memUsage);
					msgJson.put("mem_percent", memPercent);
					String msg = msgJson.toString();
					LOG.info("send message of memory percentage "
							+ "exceeding the threshold:" + msg);
					producer.produce(containerId, msg);
					
					LOG.info("wait 10 seconds for a new container to start");
					Thread.sleep(10000);
				}
			}

			pidBr.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			LOG.error("pid file not exist", e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error("read pid file error", e);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			LOG.error("thread interrupted error", e);
		}
		
	}
	
	public void stopMonitor() {
		producer.close();
	}
	
	public void startMonitorThread(String containerId) {
		
		Thread thread = new Thread(new ContainerMonitorRunnable(containerId));
		thread.start();
		
	}
	
	private class ContainerMonitorRunnable implements Runnable {
		
		private String containerId;
		
		public ContainerMonitorRunnable(String containerId) {
			this.containerId = containerId;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			monitor(containerId);
		}
		
	}

}
