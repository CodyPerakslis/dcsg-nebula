/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
package org.apache.edgent.samples.connectors.mqtt;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.edgent.connectors.mqtt.MqttConfig;
import org.apache.edgent.connectors.mqtt.MqttStreams;
import org.apache.edgent.console.server.HttpServer;
import org.apache.edgent.providers.development.DevelopmentProvider;
import org.apache.edgent.samples.connectors.Util;
import org.apache.edgent.topology.TStream;
import org.apache.edgent.topology.TWindow;
import org.apache.edgent.topology.Topology;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * A simple MQTT subscriber topology application.
 */
public class edgentapp_mod {
    private final Properties props;
    private final String topic;
    private final String outputp;

    public static void main(String[] args) throws Exception {
        if (args.length != 1)
            throw new Exception("missing pathname to mqtt.properties file");
        edgentapp_mod subscriber = new edgentapp_mod(args[0],args[1]);
        subscriber.run();
    }

    /**
     * @param mqttPropsPath pathname to properties file
     */
    edgentapp_mod(String mqttPropsPath, String outputpath) throws Exception {
        props = new Properties();
        props.load(Files.newBufferedReader(new File(mqttPropsPath).toPath()));
        topic = props.getProperty("mqtt.topics");
        outputp=outputpath;
    }
    
    private MqttConfig createMqttConfig() {
        MqttConfig mqttConfig = MqttConfig.fromProperties(props);
        return mqttConfig;
    }
    
    /**
     * Create a topology for the subscriber application and run it.
     */
    private void run() throws Exception {
    	String fileName = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    	fileName = "output_"+fileName + ".txt";
    	PrintStream out = new PrintStream(new FileOutputStream(outputp+fileName));
        System.setOut(out);
        
         DevelopmentProvider tp2 = new DevelopmentProvider();
		 Topology t2 = tp2.newTopology("mqttSamplePublisher");
	     MqttConfig mqttConfig2 = createMqttConfig();
	     MqttStreams mqtt2 = new MqttStreams(t2, () -> mqttConfig2);
	     List<String> cmdpub=new ArrayList<String>();
	     cmdpub.add("sendData");
	     TStream<String> pubstrm = t2.collection(cmdpub);
	     System.out.println("Publishing Data to Stream");
	     mqtt2.publish(pubstrm, "sensorcmd", 0/*qos*/, false/*retain*/);
	     tp2.submit(t2);
        
        
        DevelopmentProvider tp = new DevelopmentProvider();
        
        // build the application/topology
        
        Topology t = tp.newTopology("mqttSampleSubscriber");
        
        // System.setProperty("javax.net.debug", "ssl"); // or "all"; "help" for full list
        String[] tps = new String[100];
        tps=topic.split(",");
        System.out.println("Topic "+tps[0]);
        System.out.println("Topic "+tps[1]);
        System.out.println("Topic "+tps[2]);
        System.out.println("Topic "+tps[3]);
        List<String> topics = Arrays.asList(tps);
        ArrayList<TStream<String>> msgs=new ArrayList<TStream<String>>();
        ArrayList<TStream<String>> filteredmsgs=new ArrayList<TStream<String>>();
        
        for(int x=0;x<topics.size();x++){
        	MqttConfig mqttConfig = createMqttConfig();
            MqttStreams mqtt = new MqttStreams(t, () -> mqttConfig);
            
            
            // Subscribe to the topic and create a stream of messages
            TStream<String> m = mqtt.subscribe(topics.get(x), 0/*qos*/);
            msgs.add(m);
            TStream<String> n = m.filter(tuple -> Integer.parseInt(tuple) < 50 || Integer.parseInt(tuple) > 80);
            filteredmsgs.add(n);
            String y=topics.get(x);
            System.out.println("x = "+x);
            filteredmsgs.get(x).sink(tuple -> System.out.println(
                    String.format("[%s] Topicwise Alarming Values Recieved from [%s] : %d", Util.simpleTS(),y,Integer.parseInt(tuple))));
        }
        
//        
//        // Create the MQTT broker connector
//        MqttConfig mqttConfig = createMqttConfig();
//        MqttStreams mqtt = new MqttStreams(t, () -> mqttConfig);
//        
//        // Subscribe to the topic and create a stream of messages
//        TStream<String> msgs = mqtt.subscribe("simple1", 0/*qos*/);
//        
//        // Create the MQTT broker connector
//        MqttConfig mqttConfig2 = createMqttConfig();
//        MqttStreams mqtt2 = new MqttStreams(t, () -> mqttConfig2);
//        
//        // Subscribe to the topic and create a stream of messages
//        TStream<String> msgs2 = mqtt2.subscribe("simple2", 0/*qos*/);
       
        
//        TStream<String> strm1= msgs.get(0).filter(tuple -> Integer.parseInt(tuple) < 50 || Integer.parseInt(tuple) > 80);
//        TStream<String> strm2= msgs.get(1).filter(tuple -> Integer.parseInt(tuple) < 50 || Integer.parseInt(tuple) > 80);
       
        Set<TStream<String>> strmnew = new HashSet<TStream<String>>(msgs);
        TStream<String> nn = msgs.get(0);
        TStream<String> mm=nn.union(strmnew);
        TStream<Double> doubles = mm.map(Double::valueOf);
        TStream<Double> max = doubles.last(5, TimeUnit.SECONDS, tuple -> 0).aggregate((tuple, key) -> {return Collections.max(tuple);});
//        nn=nn.filter(tuple -> Integer.parseInt(tuple) < 50 || Integer.parseInt(tuple) > 80);
        // msgs.sink(tuple -> executecode(tuple));
        // Process the received msgs - just print them out
        max.sink(tuple -> System.out.println(tuple));
//        nn.sink(tuple -> System.out.println(
//              String.format("[%s] Union Alarming Values Recieved: %d", Util.simpleTS(), Integer.parseInt(tuple))));
//        max.sink(tuple -> System.out.println(
//                String.format("[%s] Max of last 5 seconds %d", Util.simpleTS(), tuple)));
//        strm1.sink(tuple -> System.out.println(
//                String.format("[%s] Stream 1 Alarming Values Recieved: %d", Util.simpleTS(), Integer.parseInt(tuple))));
//        strm2.sink(tuple -> System.out.println(
//                String.format("[%s] Stream 2 Alarming Values Recieved: %d", Util.simpleTS(), Integer.parseInt(tuple))));
        //strm.print();
        
        // run the application / topology
        System.out.println("Console URL for the job: "
                + tp.getServices().getService(HttpServer.class).getConsoleUrl());
        tp.submit(t);
    }

}
