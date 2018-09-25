package org.apache.edgent.samples.connectors.mqtt;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.edgent.connectors.mqtt.MqttConfig;
import org.apache.edgent.console.server.HttpServer;
import org.apache.edgent.providers.direct.DirectProvider;
import org.apache.edgent.topology.TStream;
import org.apache.edgent.topology.Topology;

import CompiledStatements.Tuple;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import ConstellationClient.ConstellationConnection;
import ConstellationClient.ConstellationResult;

public class EdgentClientCallOnPhotonResult {
	 private final Properties props;

	    public static void main(String[] args) throws Exception {
	        if (args.length != 1)
	            throw new Exception("missing pathname to mqtt.properties file");
	        EdgentClientCallOnPhotonResult subscriber = new  EdgentClientCallOnPhotonResult(args[0]);
	        subscriber.run();
    }

    EdgentClientCallOnPhotonResult(String mqttPropsPath) throws Exception {
        props = new Properties();
        props.load(Files.newBufferedReader(new File(mqttPropsPath).toPath()));
        String topic = props.getProperty("mqtt.topics");
    }
    
    private MqttConfig createMqttConfig() {
        MqttConfig mqttConfig = MqttConfig.fromProperties(props);
        return mqttConfig;
    }
    void run() throws Exception {
    	PrintStream out = new PrintStream(new FileOutputStream("/Users/ayushi/Downloads/Edgent_Const_OP.txt"));
        System.setOut(out);
        PhotonDriverClient sensor = new PhotonDriverClient();
        DirectProvider dp = new DirectProvider();
        Topology topology = dp.newTopology();
        TStream<Double> tempReadings = topology.poll(sensor, 1, TimeUnit.MILLISECONDS);
        TStream<Double> filteredReadings = tempReadings.filter(reading -> reading < 5 || reading > 80);
        filteredReadings.sink(tuple -> System.out.println(tuple));
        dp.submit(topology);
    }
}