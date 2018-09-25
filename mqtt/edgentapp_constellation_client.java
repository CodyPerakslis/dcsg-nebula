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
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Random;

import ConstellationClient.ConstellationConnection;
import ConstellationClient.ConstellationResult;

public class edgentapp_constellation_client {
	 private final Properties props;

	    public static void main(String[] args) throws Exception {
	        if (args.length != 1)
	            throw new Exception("missing pathname to mqtt.properties file");
	        edgentapp_constellation_client subscriber = new  edgentapp_constellation_client(args[0]);
	        subscriber.run();
	        int trials = 10;
	        Random rand = new Random();
            for (int i=0; i < trials; ++i) {

                    ConstellationConnection connection = new ConstellationConnection();
                    connection.connect("127.0.0.1");

                    ConstellationResult r1 = connection.query("FIND DEVICES WITH Temperature AS TEMPS");
                    r1.pull();
                    ArrayList<ConstellationResult> results = new ArrayList<ConstellationResult>();

                    int tasks = 1;

                    long start = System.currentTimeMillis();

                    for (int j=0; j < tasks; ++j) {

                            
                    	int period = 2500 + (rand.nextInt(5) * (j + 1) * 500);
    					results.add(connection.query("SENSE Temperature FROM TEMPS PERIOD " + period + " MS DEADLINE 300 MS DELTA 0.0001"));
    					System.out.println("Sent task with period = " + period);

                            try {
                                    Thread.sleep(500);
                            } catch (InterruptedException e) {
                                    // TODO Auto-generated catch block
                                    e.printStackTrace();
                            }
                    }

                    while (System.currentTimeMillis() - start < 300000) {

                            for (int j=0; j < tasks; ++j) {

                                    results.get(j).pull();
                            }
                    }

                    connection.disconnect();
            }

            System.out.println("Done!");
    }

    edgentapp_constellation_client(String mqttPropsPath) throws Exception {
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
        PhotonDevice sensor = new PhotonDevice();
        DirectProvider dp = new DirectProvider();
        Topology topology = dp.newTopology();
        TStream<Double> tempReadings = topology.poll(sensor, 1, TimeUnit.MILLISECONDS);
        TStream<Double> filteredReadings = tempReadings.filter(reading -> reading < 50 || reading > 80);
        filteredReadings.sink(tuple -> System.out.println(tuple));
        dp.submit(topology);
    }
}