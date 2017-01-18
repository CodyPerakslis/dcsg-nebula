package edu.umn.cs.Nebula.application;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Random;

public class MatrixConvolution extends Application{
	private static final int matrixSize = 1024;
	private static final int maskSize = 5;
	
	private static float[] matrix = new float[matrixSize*matrixSize];
	private static float[] mask = new float[maskSize*maskSize];
	private static float[] result = new float[matrixSize*matrixSize];
	private static Random rand = new Random();

	private static void initMatrix(String matrixPath, String maskPath) throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter(matrixPath));
		for (int i = 0; i < matrixSize*matrixSize; i++) {
			matrix[i] = rand.nextFloat();
			writer.write(matrix[i] + " ");
		}
		writer.flush();
		writer.close();
		
		writer = new BufferedWriter(new FileWriter(maskPath));
		for (int i = 0; i < maskSize*maskSize; i++) {
			mask[i] = rand.nextFloat();
			writer.write(matrix[i] + " ");
		}
		writer.flush();
		writer.close();
	}
	
	private static void compute() {
		for (int i = 0; i < matrixSize; ++i){
	        for (int j = 0; j < matrixSize; ++j) {
				float sum = 0;
				// check the start and end values of m and n to prevent overrunning the matrix edges
				int mbegin = (i < 2)? 2 - i : 0;
				int mend = (i > (matrixSize - 3))? matrixSize - i + 2 : maskSize;
				int nbegin = (j < 2)? 2 - j : 0;
				int nend = (j > (matrixSize - 3))? (matrixSize-j) + 2 : maskSize;
				// overlay A over B centered at element (i,j).  For each
				// overlapping element, multiply the two and accumulate
				for(int m = mbegin; m < mend; ++m) {
					for(int n = nbegin; n < nend; n++) {
						sum += mask[m * maskSize + n] * matrix[matrixSize*(i + m - 2) + (j+n - 2)];
					}
				}
				result[i*matrixSize + j] = sum;
	        }
		}
	}
	
	public static void main(String[] args) throws UnknownHostException, IOException {
		if (args.length < 2) {
			System.out.println("Params: matrixFile, maskFile");
			return;
		}
		initMatrix(args[0], args[1]);
		
		getLocationInformation();
		getLocalNodes();
		boolean success = false;

		if (nodes.isEmpty()) {
			System.out.println("No available nodes");
			return;
		}
		
		String node = nodes.getFirst();
		long time = System.currentTimeMillis();
		success = sendFile("matrix.txt", args[0], node, 6425);
		// if (success)
		//	success = sendFile("mask.txt", args[1], node, 6425);
		System.out.println("Sending matrices completed in " + (System.currentTimeMillis() - time) + " ms");
		
		if (!success) {
			System.out.println("Failed sending data to " + node);
			return;
		}
		
		time = System.currentTimeMillis();
		compute();
		System.out.println("Computing 2D convolution done in " + (System.currentTimeMillis() - time) + " ms");
	}
}
