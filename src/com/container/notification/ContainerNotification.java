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
	
	private String nodeHost;
	
	public ContainerNotification(String[] args) {
		Properties prop = new Properties();
		InputStream input = ContainerNotification.class.
				getClassLoader().getResourceAsStream("monitor.properties");
		
		try {
			prop.load(input);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error("read properties file error", e);
		}
		
		monitor = new ContainerResourceMonitor();
		nodeHost = monitor.init(args);
		
		String host = prop.getProperty("kafka.host");
		String port = prop.getProperty("kafka.port");
		String groupId = prop.getProperty("group.id");
		String topic = prop.getProperty("container.id.topic");
		
		consumer = new KafkaConsumerClient<String, String>
			(host + ":" + port, groupId + "-" + nodeHost);
		consumer.addTopic(topic);
		
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
				LOG.info("receive container id message:" + containerIdMsg);
				JSONObject containerIdJson = new JSONObject(containerIdMsg);
				if(containerIdJson.has("finish")){
					finish = true;
					LOG.info("finish the notifier");
					break;
				}
				if(containerIdJson.getString("node_host").equals(nodeHost)){
					String containerId = containerIdJson.getString("container_id");
					String containerMem = containerIdJson.getString("container_memory");
					LOG.info("start a thread to monitor container " + containerId);
					LOG.info("the allocated memory of the container is " + containerMem);
					monitor.startMonitorThread(containerId, Integer.parseInt(containerMem));
				}	
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
		
		ContainerNotification cn = new ContainerNotification(args);
		cn.startNotify();
		
		cn.stopNotify();
		System.exit(0);

	}

}
