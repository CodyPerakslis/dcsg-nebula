package org.apache.edgent.samples.connectors.mqtt;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.apache.edgent.function.Supplier;
import CompiledStatements.Tuple;
import CompilerErrors.CompilerError;
import ConstellationClient.ConstellationConnection;
import ConstellationClient.ConstellationResult;

public class PhotonDriverClient implements Supplier<Double>{
	 double currentTemp = 65.0;
	    Random rand;
	    ConstellationConnection connection;
	    ConstellationResult r1;
	    ArrayList<ConstellationResult> results;
	    List<String> strs = new ArrayList<String>();
	    Iterator<Tuple> tuples;

	    PhotonDriverClient(){
	        
	    	rand = new Random();
	        connection = new ConstellationConnection();
            
	        try {
            	
				connection.connect("127.0.0.1");
				
				r1 = connection.query("FIND DEVICES WITH Temperature AS TEMPS");
			    r1.pull();
            results = new ArrayList<ConstellationResult>();
            
            int tasks = 1;

            long start = System.currentTimeMillis();

            for (int j=0; j < tasks; ++j) {

                    
            	int period = 25 + (rand.nextInt(5) * (j + 1) * 5);

					results.add(connection.query("SENSE Temperature FROM TEMPS PERIOD " + period + " MS DEADLINE 300 MS DELTA 0.0001"));
					System.out.println("Sent task with period = " + period);
					Thread.sleep(5);
            }
            
	        
            
	        while (System.currentTimeMillis() - start < 3000) {

                        results.get(0).pull();
        }
	        
            tuples = results.get(0).getIterator();
            
            while (tuples.hasNext())
            {
            	Tuple g = tuples.next();
            	strs.add(g.getValue());
            }
            
            connection.disconnect();
            
	        } catch (UnknownHostException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (CompilerError e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            
	    }

	    @Override
	    public Double get() {
	    	Double val = Double.parseDouble(strs.get(0));
	    	strs.remove(0);
	    	return val;
	        
	    }

}
