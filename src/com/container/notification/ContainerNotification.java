package com.container.notification;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.JSONObject;

import com.container.monitor.ContainerResourceMonitor;
import com.kafka.client.KafkaConsumerClient;

public class ContainerNotification {
	
	private static final Log LOG = LogFactory.getLog(ContainerNotification.class);
	
	private KafkaConsumerClient<String, String> consumer;
	
	private ContainerResourceMonitor monitor;
	
	public ContainerNotification() {
		Properties prop = new Properties();
		InputStream input = ContainerNotification.class.
				getClassLoader().getResourceAsStream("monitor.properties");
		
		try {
			prop.load(input);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error("read properties file error", e);
		}
		
		String host = prop.getProperty("kafka.host");
		String port = prop.getProperty("kafka.port");
		String groupId = prop.getProperty("group.id");
		String topic = prop.getProperty("container.id.topic");
		
		consumer = new KafkaConsumerClient<String, String>(host + ":" + port, groupId);
		consumer.addTopic(topic);
		
		monitor = new ContainerResourceMonitor();
	}
	
	public void startNotify() {
		LOG.info("start notifying");
		consumer.subscibe();
		boolean finish = false;
		while(!finish){
			finish = notifyContainer(consumer.consume());
		}
		
	}
	
	public boolean notifyContainer(ConsumerRecords<String, String> records) {
		
		boolean finish = false;
		
		for(ConsumerRecord<String, String> record : records){
			if(record.value()!=null){
				String containerIdMsg = record.value().toString();
				JSONObject containerIdJson = new JSONObject(containerIdMsg);
				if(containerIdJson.has("finish")){
					finish = true;
					LOG.info("finish the notifier");
				}
				String containerId = containerIdJson.getString("container_id");
				LOG.info("start a thread to monitor container " + containerId);
				monitor.startMonitorThread(containerId);
			}
		}
		
		return finish;
		
	}
	
	public void stopNotify() {
		LOG.info("stop notifying");
		monitor.stopMonitor();
		consumer.close();
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		ContainerNotification cn = new ContainerNotification();
		cn.startNotify();
		
		cn.stopNotify();
		System.exit(0);

	}

}
