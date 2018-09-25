package org.apache.edgent.samples.connectors.mqtt;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.edgent.connectors.mqtt.MqttConfig;
import org.apache.edgent.connectors.mqtt.MqttStreams;
import org.apache.edgent.console.server.HttpServer;
import org.apache.edgent.providers.development.DevelopmentProvider;
import org.apache.edgent.samples.connectors.Util;
import org.apache.edgent.topology.TStream;
import org.apache.edgent.topology.Topology;

public class autopublish2 {
	
	    private final Properties props;
	    //private final String topic;
	    private String pathname;
	    private String machineip;
	    private String port;
	    private String topic;
	    private String seconds;

	    public static void main(String[] args) throws Exception {
//	        if (args.length != 1)
//	            throw new Exception("missing pathname to mqtt.properties file");
	        autopublish2 subscriber = new autopublish2(args[0]);
	        subscriber.pathname=args[1];
	        subscriber.machineip=args[2];
	        subscriber.port=args[3];
	        subscriber.topic=subscriber.props.getProperty("mqtt.topic");
	        subscriber.seconds=args[4];
	        subscriber.run();
	    }

	    /**
	     * @param mqttPropsPath pathname to properties file
	     */
	    autopublish2(String mqttPropsPath) throws Exception {
	        props = new Properties();
	        props.load(Files.newBufferedReader(new File(mqttPropsPath).toPath()));
	        topic = props.getProperty("mqtt.topic");
	    }
	    
	    private MqttConfig createMqttConfig() {
	        MqttConfig mqttConfig = MqttConfig.fromProperties(props);
	        return mqttConfig;
	    }
	    
	    private void executecode(String t, String path, String ip, String port, String topic, String seconds){
	    	if(t.equalsIgnoreCase("sendData")){
	    		try {
	    		 System.out.println("Sensor got command to publish");
	    		 DevelopmentProvider tp = new DevelopmentProvider();
	    		 Topology t2 = tp.newTopology("mqttSamplePublisher");
	    	     MqttConfig mqttConfig2 = createMqttConfig();
	    	     MqttStreams mqtt2 = new MqttStreams(t2, () -> mqttConfig2);
	    	     List<String> cmdpub=new ArrayList<String>();
	    	     String[] cmd = { "sh "+path+" "+ip+" "+port+" "+topic+" "+seconds };
	    		 Process script_exec = Runtime.getRuntime().exec(cmd);
				 script_exec.waitFor();
	    		  if(script_exec.exitValue() != 0){
	    		   System.out.println("Error while executing script");
	    		  }
	    		  BufferedReader stdInput = new BufferedReader(new
	    		                InputStreamReader(script_exec.getInputStream()));
	    		 } catch (Exception e) {
						e.printStackTrace();
					}
	    		  }
	    }
	    /**
	     * Create a topology for the subscriber application and run it.
	     */
	    private void run() throws Exception {
	        DevelopmentProvider tp = new DevelopmentProvider();
	        
	        // build the application/topology
	        
	        Topology t = tp.newTopology("mqttSampleSubscriber");
	    
	        MqttConfig mqttConfig = createMqttConfig();
	        MqttStreams mqtt = new MqttStreams(t, () -> mqttConfig);
	        
	        // Subscribe to the topic and create a stream of messages
	        TStream<String> msgs = mqtt.subscribe(props.getProperty("mqtt.topic"), 0/*qos*/);
	        
//	        TStream<String> strm= msgs.filter(tuple -> Integer.parseInt(tuple) < 50 || Integer.parseInt(tuple) > 80);
	        msgs.sink(tuple -> executecode(tuple,pathname,machineip,port,topic,seconds));
	        
	        System.out.println("Console URL for the job: "
	                + tp.getServices().getService(HttpServer.class).getConsoleUrl());
	        tp.submit(t);
	    }

	}
