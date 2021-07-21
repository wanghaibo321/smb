package adaptiveVirtualBitmapstrict2;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;


/** A general framework for count min. The elementary data structures to be shared here can be counter, bitmap, FM sketch, HLL sketch. Specifically, we can
 * use counter to estimate flow sizes, and use bitmap, FM sketch and HLL sketch to estimate flow cardinalities
 * @author Jay, Youlin, 2018. 
 */

public class singleFlowEstimate {
	public static Random rand = new Random();
	
	public static int n = 0; 						// total number of packets
	public static int flows = 0; 					// total number of flows
	public static int avgAccess = 0; 				// average memory access for each packet
	public static final int M = 1024 * 1024 * 8; 	// total memory space Mbits	
	public static GeneralDataStructure C;
	public static Set<Integer> sizeMeasurementConfig = new HashSet<>(Arrays.asList()); // -1-regular CM; 0-enhanced CM; 1-Bitmap; 2-FM sketch; 3-HLL sketch
	
	public static Set<Integer> spreadMeasurementConfig = new HashSet<>(Arrays.asList(4)); // 1-Bitmap; 2-FM sketch; 3-HLL sketch; 4-Gbitmap (also called self-morphing bitmap in the paper); 5-MRBitmap
	
	public static Set<Integer> expConfig = new HashSet<>(Arrays.asList()); //0-ECountMin dist exp
	public static boolean isGetThroughput = false;
	
	/** parameters for count-min */
	public static final int d = 1; 			// the number of rows in Count Min
	public static int w = 1;				// the number of columns in Count Min
	public static int u = 1;				// the size of each elementary data structure in Count Min.
	public static int[] S = new int[d];		// random seeds for Count Min
	public static int m = 1;				// number of bit/register in each unit (used for bitmap, FM sketch and HLL sketch)

	public static int maxSpread = 20000;
	/** parameters for counter */
	public static int mValueCounter = 1;			// only one counter in the counter data structure
	public static int counterSize = 32;				// size of each unit

	/** parameters for bitmap */
	public static final int bitArrayLength =4096;
	
	/** parameters for FM sketch **/
	public static int mValueFM = 128;
	public static final int FMsketchSize = 32;
	
	/** parameters for HLL sketch **/
	public static int mValueHLL =1000;
	public static final int HLLSize = 5;

	/** parameters for GBitmap sketch **/
	public static int mValueGBitmap =5000;
	public static final int TGBitmap =500;
	public static int mMRBitmap =5000;
	public static final int rMRBitmap =500;
	public static int times = 0;
	public static int repeatTime = 10;
	public static int range = 500;
	public static int stepLength = 100;
	/** number of runs for throughput measurement */
	public static int loops = 100;
	public static int numOfFile = 10;
	
	public static void main(String[] args) throws FileNotFoundException {
		/** measurement for flow sizes **/
		
		double haha =0.0;
		for (int i=2;i<7;i++) {
			haha = 0;
			double max_=Math.pow(10, i);
			for (int j =1;j<=max_;j++)
				haha +=1*1.0/j;
			System.out.println("1+1/2+,...,1/"+(int)(max_)+"\t =\t"+haha);
			
		}
		System.out.println("Start****************************");
		/** measurement for flow sizes **/
		
		/** measurement for flow spreads **/
			HashMap<String, ArrayList<String>> mp_spread = new HashMap<>();
			HashMap<String, HashSet<String>> mp_spread1 = new HashMap<>();

				if (true) {

			for (int ii=0;ii<numOfFile;ii++) {
				///mp_spread = encodeFile(mp_spread,GeneralUtil.path+"\\CAIDA\\output"+ii+"v.txt");
				mp_spread = encodeFile(mp_spread,GeneralUtil.path+"\\CAIDA\\srcDstSize"+ii+"v.txt");

			}
			for (int ii=0;ii<numOfFile;ii++) {
				//mp_spread1 = encodeFileSpread(mp_spread1, GeneralUtil.path+"\\CAIDA\\output"+ii+"v.txt");
				mp_spread1 = encodeFileSpread(mp_spread1, GeneralUtil.path+"\\CAIDA\\srcDstSize"+ii+"v.txt");

			}
			System.out.println("finish loading data to hashmap");
		}			
				HashMap<String, Integer> mp = new HashMap<String, Integer>();

		for (int i : spreadMeasurementConfig) {
			initCM(i);
			if (false) {// this is the experiment under caida dataset, where all packets to the same destination forms a flow/data stream
			String resultFilePath = GeneralUtil.path + "adaptiveVirtualBitmap\\strict_" + C.getDataStructureName()
					+  "_m_" + m+"datasetnum_"+numOfFile;
			PrintWriter pw = new PrintWriter(new File(resultFilePath));
			mp.clear();
			System.out.println(mp_spread.size());
			for (Map.Entry<String, ArrayList<String>> flow: mp_spread.entrySet()) {//for each flow
				
				initCM(i);
				for (int ii=0;ii<flow.getValue().size();ii++) {// for each element or called data item in the flow
					C.encode(flow.getValue().get(ii));
				}
				
				mp.put((flow.getKey()+"\t"+mp_spread1.get(flow.getKey()).size()), C.getValue());
			}
			Map<String, Integer> result = GeneralDataProcessing.sortByValue(mp);
			for (Map.Entry<String, Integer> entry: result.entrySet()) {
				pw.println(entry.getKey() + "\t" + entry.getValue());
			}
			pw.close(); 
			GeneralUtil.analyzeAccuracy(resultFilePath);
			}
			if (true) {//------------------------- this is the experiments for data stream with given cardinality-----------------------------
				double duration = 0;

				String resultFilePath = GeneralUtil.path + "adaptiveVirtualBitmap\\strict_randomflow_" + C.getDataStructureName()
				+  "_m_" + m+"range_"+range*stepLength;
		PrintWriter pw = new PrintWriter(new File(resultFilePath));
				for (int count = 0;count<range;count++) {
			
				int tempValue = 0;					
				int elemNum = Math.max(count*stepLength,1);

				for (int times = 0;times<repeatTime;times++) {
					initCM(i);
					HashSet<Long> ElemID = generateRandomElem(elemNum);//generate a data stream with the cardinality of elemNum
					
					for(Long element:ElemID) {
						C.encode(Long.toString(element));
					}						
				long startTime = System.nanoTime();
				
						tempValue = C.getValue();					
					long endTime = System.nanoTime();

					duration += 1.0 * (endTime - startTime) / 1000000000;

					pw.println(count + "\t" +elemNum+"\t"+ tempValue);

				}				
				}	
				
				long startTime = System.nanoTime();
				for ( i=0;i<1000000;i++)
				 C.getValue();					
			long endTime = System.nanoTime();

			duration += 1.0 * (endTime - startTime) / 1000000000;
				System.out.println(C.getDataStructureName() + "\t encoding Average Throughput: " + 0.5 * range*range*repeatTime / (duration ) + " packets/second" );
				System.out.println(C.getDataStructureName() + "\t query Average Throughput: " + 1.0*range*repeatTime / (duration ) + " packets/second" );
				System.out.println(C.getDataStructureName() + "\t query Average Throughput: " + 1.0*1000000 / (duration ) + " packets/second" );


				pw.close();
				
				GeneralUtil.analyzeAccuracy(resultFilePath);

								
			}
			
		}
		
		/** experiment for specific requirement *
		for (int i : expConfig) {
			switch (i) {
	        case 0:  initCM(0);
					 encodeSize(GeneralUtil.dataStreamForFlowSize);
					 randomEstimate(10000000);
	                 break;
	        default: break;
			}
		}*/
		System.out.println("DONE!****************************");
	}
public static HashMap<String, HashSet<String>> encodeFileSpread(HashMap<String, HashSet<String>> mp, String path) throws FileNotFoundException{
	Scanner sc = new Scanner(new File(path));
	while(sc.hasNext()) {
		String entry = sc.nextLine();
		String[] strs = entry.split("\\s+");
		String srcIP = strs[0], dstIP = strs[1];
		if (!mp.containsKey(dstIP)) mp.put(dstIP, new HashSet<String> ());
		mp.get(dstIP).add(srcIP);
	}
	sc.close();
	return mp;

}
	public static HashMap<String,  ArrayList<String>> encodeFile(HashMap<String,  ArrayList<String>> mp, String path) throws FileNotFoundException{
		Scanner sc = new Scanner(new File(path));
		while(sc.hasNext()) {
			String entry = sc.nextLine();
			String[] strs = entry.split("\\s+");
			String srcIP = strs[0], dstIP = strs[1];
			if (!mp.containsKey(dstIP)) mp.put(dstIP, new ArrayList<String>());
			mp.get(dstIP).add(srcIP);
		}
		sc.close();
		return mp;

	}
	public static HashSet<Long> generateRandomElem(int num) {
		
		HashSet<Long> elem = new HashSet<Long> ();
		int i=0;
		while(i<num) {
			long tempElem = rand.nextLong();
			if (!elem.contains(tempElem)) {
				elem.add(tempElem);
				i++;
			}
		}
		return elem;
	}
	// Init the Count Min for different elementary data structures.
	public static void initCM(int index) {
		switch (index) {
	        case 0: case -1: C = generateCounter();
	                 break;
	        case 1:  C = generateBitmap();
	                 break;
	        case 2:  C = generateFMsketch();
	                 break;
	        case 3:  C = generateHyperLogLog();
	                 break;
	        case 4:  C = generateGBitmap();
	        		 break;
	        case 5:  C = generateMRBitmap();
	        		 break;
	        default: break;
		}
		//System.out.println("\nCount Min-" + C.getDataStructureName() + " Initialized!");
	}
	
	// Generate counter base Counter Min for flow size measurement.
	public static Counter generateCounter() {
		m = mValueCounter;
		u = counterSize * mValueCounter;
		w = (M / u) / d;
		Counter B= new Counter(1, counterSize);
		
		return B;
	}
	
	// Generate bitmap base Counter Min for flow cardinality measurement.
	public static Bitmap generateBitmap() {
		m = bitArrayLength;
		u = bitArrayLength;
		w = (M / u) / d;
		Bitmap B =new Bitmap(bitArrayLength);			
		return B;
	}
	
	// Generate FM sketch base Counter Min for flow cardinality measurement.
	public static FMsketch generateFMsketch() {
		m = mValueFM;
		u = FMsketchSize * mValueFM;
		w = (M / u) / d;
		FMsketch B  = new FMsketch(mValueFM, FMsketchSize);
		return B;
	}
	
	// Generate HLL sketch base Counter Min for flow cardinality measurement.
	public static HyperLogLog generateHyperLogLog() {
		m = mValueHLL;
		u = HLLSize * mValueHLL;
		w = (M / u) / d;
		HyperLogLog B =new HyperLogLog(mValueHLL, HLLSize);
		return B;
	}
	public static GBitmap generateGBitmap() {
		m = mValueGBitmap;
		GBitmap B = new GBitmap(mValueGBitmap, TGBitmap);
		return B;
	}
	public static MRBitmap generateMRBitmap() {
		m = mValueGBitmap;
		MRBitmap B = new MRBitmap(rMRBitmap, mMRBitmap/rMRBitmap);
		return B;
	}
	
	// Generate random seeds for Counter Min.
	
	
	/** Encode elements to the Count Min for flow spread measurement. */
	
	

	/** Get throughput for flow size measurement. */
	
}
