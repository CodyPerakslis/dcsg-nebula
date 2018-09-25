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

public class autopublisher {
	
	    private final Properties props;
	    private final String topic;

	    public static void main(String[] args) throws Exception {
//	        if (args.length != 1)
//	            throw new Exception("missing pathname to mqtt.properties file");
	        autopublisher subscriber = new autopublisher(args[0]);
	        subscriber.run();
	    }

	    /**
	     * @param mqttPropsPath pathname to properties file
	     */
	    autopublisher(String mqttPropsPath) throws Exception {
	        props = new Properties();
	        props.load(Files.newBufferedReader(new File(mqttPropsPath).toPath()));
	        topic = props.getProperty("mqtt.topic");
	    }
	    
	    private MqttConfig createMqttConfig() {
	        MqttConfig mqttConfig = MqttConfig.fromProperties(props);
	        return mqttConfig;
	    }
	    
	    private void executecode(String t){
	    	if(t.equalsIgnoreCase("sendData")){
	    		try {
	    		 System.out.println("Sensor got command to publish");
	    		 DevelopmentProvider tp = new DevelopmentProvider();
	    		 Topology t2 = tp.newTopology("mqttSamplePublisher");
	    	     MqttConfig mqttConfig2 = createMqttConfig();
	    	     MqttStreams mqtt2 = new MqttStreams(t2, () -> mqttConfig2);
	    	     List<String> cmdpub=new ArrayList<String>();
	    	     while(true){
	    	     int random = (int )(Math.random() * 1000 + 1);
	    	     cmdpub.add(new Integer(random).toString());
	    	     TStream<String> pubstrm = t2.collection(cmdpub);
	    	     System.out.println("Publishing Data to Stream");
	    	     mqtt2.publish(pubstrm, "sensorcmd", 0/*qos*/, false/*retain*/);
	    	     tp.submit(t2);
	    	     }
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
	        msgs.sink(tuple -> executecode(tuple));
	        
	        System.out.println("Console URL for the job: "
	                + tp.getServices().getService(HttpServer.class).getConsoleUrl());
	        tp.submit(t);
	    }

	}
