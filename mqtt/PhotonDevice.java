package org.apache.edgent.samples.connectors.mqtt;
import java.util.Random;
import org.apache.edgent.function.Supplier;
public class PhotonDevice implements Supplier<Double>{
	 double currentTemp = 65.0;
	    Random rand;

	    PhotonDevice(){
	        rand = new Random();
	    }

	    @Override
	    public Double get() {
	        // Change the current temperature some random amount
	        double newTemp = rand.nextGaussian() + currentTemp;
	        currentTemp = newTemp;
	        return currentTemp;
	    }

}
