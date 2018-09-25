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
public class CommandSubscriberApp {
	
	    private final Properties props;
	    private final String topic;

	    public static void main(String[] args) throws Exception {
//	        if (args.length != 1)
//	            throw new Exception("missing pathname to mqtt.properties file");
	        CommandSubscriberApp subscriber = new CommandSubscriberApp(args[0]);
	        subscriber.run();
	    }

	    /**
	     * @param mqttPropsPath pathname to properties file
	     */
	    CommandSubscriberApp(String mqttPropsPath) throws Exception {
	        props = new Properties();
	        props.load(Files.newBufferedReader(new File(mqttPropsPath).toPath()));
	        topic = props.getProperty("mqtt.topic");
	    }
	    
	    private MqttConfig createMqttConfig() {
	        MqttConfig mqttConfig = MqttConfig.fromProperties(props);
	        return mqttConfig;
	    }
	    
	    private void executecode(String t){
	    	if(t.equalsIgnoreCase("run")){
	    		try {
	    		 System.out.println("RUN");
	    		 DevelopmentProvider tp = new DevelopmentProvider();
	    		 Topology t2 = tp.newTopology("mqttSamplePublisher");
	    	     MqttConfig mqttConfig2 = createMqttConfig();
	    	     MqttStreams mqtt2 = new MqttStreams(t2, () -> mqttConfig2);
	    	     List<String> cmdpub=new ArrayList<String>();
	    	     cmdpub.add("sendData");
	    	     TStream<String> pubstrm = t2.collection(cmdpub);
	    	     System.out.println("Publishing Data to Stream");
	    	     mqtt2.publish(pubstrm, "sensorcmd", 0/*qos*/, false/*retain*/);
	    	     tp.submit(t2);
//	    		try {
//					Process procBuildScript = new ProcessBuilder("java8/samples/src/connectors/src/main/java/org/apache/edgent/samples/connectors/mqtt/runSimpleSubscriberApp.sh").start();
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
	    	    
	    		String[] cmd = { "java8/samples/src/connectors/src/main/java/org/apache/edgent/samples/connectors/mqtt/runSimpleSubscriberApp.sh"};
	    		  Process script_exec;
				
					script_exec = Runtime.getRuntime().exec(cmd);
				
	    		  script_exec.waitFor();
	    		  if(script_exec.exitValue() != 0){
	    		   System.out.println("Error while executing script");
	    		  }
	    		  BufferedReader stdInput = new BufferedReader(new
	    		                InputStreamReader(script_exec.getInputStream()));
//	    		  String s; = stdInput.readLine();
//	    		  System.out.println("s is null????");
//	    		  if(s==null)
//	    		  System.out.println("s is null");
//	    		  else
//	    			  System.out.println("s is not null");
//	    		  System.out.println(s);
//	    		  while ((s = stdInput.readLine()) != null) {
//	    			  System.out.println("s is ???");
//	    		                System.out.println(s);
//	    		            }
	    		  
	    		  
	    		  } catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	    		  }
//	    	else System.out.println("Wasn't equal");
	    }
	    /**
	     * Create a topology for the subscriber application and run it.
	     */
	    private void run() throws Exception {
	        DevelopmentProvider tp = new DevelopmentProvider();
	        System.out.println("Working Directory = " +
	                System.getProperty("user.dir"));
	        // build the application/topology
	        
	        Topology t = tp.newTopology("mqttSampleSubscriber");
	        
	        // System.setProperty("javax.net.debug", "ssl"); // or "all"; "help" for full list

	        // Create the MQTT broker connector
	        MqttConfig mqttConfig = createMqttConfig();
	        MqttStreams mqtt = new MqttStreams(t, () -> mqttConfig);
	        
	        // Subscribe to the topic and create a stream of messages
	        TStream<String> msgs = mqtt.subscribe(props.getProperty("mqtt.topic"), 0/*qos*/);
	        
//	        TStream<String> strm= msgs.filter(tuple -> Integer.parseInt(tuple) < 50 || Integer.parseInt(tuple) > 80);
	        msgs.sink(tuple -> executecode(tuple));
	        // Process the received msgs - just print them out
//	        strm.sink(tuple -> System.out.println(
//	                String.format("[%s] received: %d", Util.simpleTS(), Integer.parseInt(tuple))));
	        
//	        strm.print();
	        
	        // run the application / topology
	        System.out.println("Console URL for the job: "
	                + tp.getServices().getService(HttpServer.class).getConsoleUrl());
	        tp.submit(t);
	    }

	}
