package adaptiveVirtualBitmapstrict2;

import java.util.BitSet;
import java.util.HashSet;

import adaptiveVirtualBitmap.GeneralUtil;

public class GBitmap extends GeneralDataStructure{
	/** parameters for bitmap */
	public int bitmapValue;										// the value derived from the bitmap
	public static String name = "GBitmap";//also called self-morphing bitmap in the paper						// bitmap data structure name
	public int arrayLength;								// the size of the bit array
	public int numOfTrue;
	public int round;
	public BitSet B;
	public int maxR;
	public int[] phaseV;
	public int T;
	public int hashT;
	public int M =(int) Math.pow(2,20);
	public GBitmap(int s, int threshold) {
		super();
		arrayLength = s;
		T = threshold;
		hashT = M;
		numOfTrue =0;
		round = 0;
		maxR = s/T;

		phaseV = new int[maxR];
		phaseV[0] = (int)(-(arrayLength) * Math.log(1.0 * Math.max((arrayLength)-T, 1)/ (arrayLength)));

		for (int i=1;i<maxR;i++) {
			phaseV[i] = phaseV[i-1]+(int)(Math.pow(2,i)*(-(arrayLength-T*i) * Math.log(1.0 * Math.max((arrayLength-T*i)-T, 1)/ (arrayLength-T*i))));
		}
		B = new BitSet(s);
	}

	@Override
	public String getDataStructureName() {
		return name;
	}

	@Override
	public int getUnitSize() {
		return arrayLength;
	}
	
	@Override
	public BitSet getBitmaps() {
		return B;
	}
	
	@Override
	public int[] getCounters() {
		return null;
	};
	
	@Override
	public BitSet[] getFMSketches() {
		return null;
	};
	
	@Override
	public BitSet[] getHLLs() {
		return null;
	};	

	@Override
	public int getValue() {
		//System.out.println(numOfTrue+"\t"+round);
		double result = 0;
		//for (int ii=0;ii<round;ii++) {
			
				//double baseBitmap = (-(arrayLength-T*ii) * Math.log(1.0 * Math.max((arrayLength-T*ii)-T, 1)/ (arrayLength-T*ii)));
				//result += (int)(Math.pow(2,ii))*arrayLength*1.0*baseBitmap/(arrayLength-T*ii);
			
		//}
		
		double baseBitmap = (-(arrayLength-T*round) * Math.log(1.0 * Math.max((arrayLength-T*round)-numOfTrue, 1)/ (arrayLength-T*round)));
		result = (int)(Math.pow(2,round))*1.0*baseBitmap;
		if (round!=0) result +=phaseV[round-1];	
		
		
		return Math.max((int)(result),1);
		
	}
	@Override
	public void encode() {
		int k = rand.nextInt(arrayLength);
		B.set(k);
	}
	public void encode(String elementID) {
		int hashValue =Math.abs( GeneralUtil.FNVHash1(elementID));
		
		if (Integer.numberOfLeadingZeros(hashValue<<1)>=round) {
			//hashValue = Math.abs( GeneralUtil.FNVHash1(elementID)^1234456);	
			hashValue =GeneralUtil.FNVHash1(elementID+"fjk");	

			int k = (hashValue % arrayLength + arrayLength) % arrayLength;
			if (!B.get(k)) {
				B.set(k);
				numOfTrue +=1;
				if (numOfTrue>=T) {
					round+=1;
					numOfTrue = 0;
				
				}
			}
		}
	}
	/*
	@Override
	public void encode(String elementID) { // for the initial version that strictly confrom 2 probability
		int hashValue =(Math.abs( GeneralUtil.FNVHash1(elementID))%M+M)%M;
		
		if (hashValue<hashT) {
			hashValue = Math.abs( GeneralUtil.FNVHash1(elementID)^1234456);		
			int k = (hashValue % arrayLength + arrayLength) % arrayLength;
			if (!B.get(k)) {
				B.set(k);
				numOfTrue +=1;
				if (numOfTrue>=T) {
					round+=1;
					hashT = (int)(M*1.0/((arrayLength-(round)*T)*Math.pow(2,round))*arrayLength);

					numOfTrue = 0;
				
				}
			}
		}
	}*/
	public boolean encodeFilter(String elementID) {
		int k = (GeneralUtil.FNVHash1(elementID) % arrayLength + arrayLength) % arrayLength;
		if (B.get(k)) return true;
		else {
			B.set(k); return false;
		}
	}
	public boolean throughFilter(long elementID,int s) {
		int k = (GeneralUtil.FNVHash1(elementID^s) % arrayLength + arrayLength) % arrayLength;
		//if (arrayLength==24) {System.out.println("heljakljfdka;" +B);
		//System.out.println(k);
		//System.out.println(!B.get(k));}
		return !B.get(k);
	}
	public boolean encodeFilter(long elementID,int s) {
		int k = (GeneralUtil.FNVHash1(elementID^s) % arrayLength + arrayLength) % arrayLength;
		if (B.get(k)) return true;
		else {
			B.set(k); return false;
		}
	}
	@Override
	public void encode(int elementID) {
		int k = (GeneralUtil.intHash(elementID) % arrayLength + arrayLength) % arrayLength;
		B.set(k);
	}
	
	@Override
	public void encode(String flowID, int[] s) {
		int m = s.length;
		int j = rand.nextInt(m);
		int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % arrayLength + arrayLength) % arrayLength;
		B.set(i);
	}
	
	@Override
	public void encodeSegment(String flowID, int[] s, int w) {
		int ms = s.length;
		int j = rand.nextInt(ms);
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
		int i = j * w + k;
		B.set(i);
	}
	
	@Override
	public void encodeSegment(long flowID, int[] s, int w) {
		int ms = s.length;
		int j = rand.nextInt(ms);
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
		int i = j * w + k;
		B.set(i);
	}
	
	@Override
	public void encodeSegment(int flowID, int[] s, int w) {
		int ms = s.length;
		int j = rand.nextInt(ms);				// (GeneralUtil.intHash(flowID) % ms + ms) % ms;		//
		int k = (GeneralUtil.intHash(flowID ^ s[j]) % w + w) % w;
		int i = j * w + k;
		B.set(i);
	}
	
	@Override
	public void encode(int flowID, int[] s) {
		int m = s.length;
		int j = rand.nextInt(m);
		int i = (GeneralUtil.intHash(flowID ^ s[j]) % arrayLength + arrayLength) % arrayLength;
		B.set(i);
	}

	@Override
	public void encode(String flowID, String elementID, int[] s) {
		int m = s.length;
		int j = (GeneralUtil.FNVHash1(elementID) % m + m) % m;
		int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % arrayLength + arrayLength) % arrayLength;
		B.set(i);
	}
	@Override
	public void encode(long flowID, long elementID, int[] s) {
		int m = s.length;
		int j = (GeneralUtil.FNVHash1(elementID) % m + m) % m;
		int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % arrayLength + arrayLength) % arrayLength;
		B.set(i);
	}
	
	@Override
	public void encode(int flowID, int elementID, int[] s) {
		int m = s.length;
		int j = (GeneralUtil.intHash(elementID) % m + m) % m;
		int i = (GeneralUtil.intHash(flowID ^ s[j]) % arrayLength + arrayLength) % arrayLength;
		B.set(i);
	}
	@Override
	public GeneralDataStructure[] getsplit(int m,int w) {
		GeneralDataStructure[]  B=new GBitmap[m];
		for(int i=0;i<m;i++) {
			B[i]=new GBitmap(w,30);
			for(int j=0;j<w/m;j++)
				B[i].getBitmaps().set(j, this.getBitmaps().get(i*(w/m)+j));
		}
		return B;
	}
	@Override
	public void encodeSegment(long flowID, long elementID, int[] s, int w) {
		int m = s.length;
		int j = (GeneralUtil.FNVHash1(elementID^flowID) % m + m) % m;
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
		int i = j * w + k;
		B.set(i);
	}
	@Override
	public void encodeSegment(String flowID, String elementID, int[] s, int w) {
		int m = s.length;
		int j = (GeneralUtil.FNVHash1(elementID) % m + m) % m;
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
		int i = j * w + k;
		B.set(i);
	}
	
	@Override
	public void encodeSegment(int flowID, int elementID, int[] s, int w) {
		int m = s.length;
		int j = (GeneralUtil.intHash(elementID) % m + m) % m;
		int k = (GeneralUtil.intHash(flowID ^ s[j]) % w + w) % w;
		int i = j * w + k;
		B.set(i);
	}
	
	@Override
	public int getValue(String flowID, int[] s) {
		int ms = s.length;
		BitSet b = new BitSet(ms);
		for (int j = 0; j < ms; j++) {
			int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % arrayLength + arrayLength) % arrayLength;
			if (B.get(i)) b.set(j);
		}
		return getValue(b);
	}
	
	@Override
	public int getValue(long flowID, int[] s) {
		int ms = s.length;
		BitSet b = new BitSet(ms);
		for (int j = 0; j < ms; j++) {
			int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % arrayLength + arrayLength) % arrayLength;
			if (B.get(i)) b.set(j);
		}
		return getValue(b);
	}
	
	@Override
	public int getValueSegment(String flowID, int[] s, int w) {
		int ms = s.length;
		BitSet b = new BitSet(ms);
		for (int j = 0; j < ms; j++) {
			int i = j * w + (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
			if (B.get(i)) b.set(j);
		}
		return getValue(b);
	}
	
	@Override
	public int getValueSegment(long flowID, int[] s, int w) {
		int ms = s.length;
		BitSet b = new BitSet(ms);
		for (int j = 0; j < ms; j++) {
			int i = j * w + (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
			if (B.get(i)) b.set(j);
		}
		return getValue(b);
	}
	
	/**
	 * reduce the number of estimator used to reduce bias for small flows.
	 */
	@Override
	public int getOptValueSegment(String flowID, int[] s, int w, int sample_ratio) {
		int ms = s.length;
		int[] indexes = new int[ms];
		for (int j = 0; j < ms; j++) {
			int i = j * w + (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
			indexes[j] = i;
		}
		int mss = ms / sample_ratio, i = 0;
		BitSet sketch_sampled = new BitSet(mss);
		HashSet<Integer> index_sampled = new HashSet<>();
		while (index_sampled.size() < mss) {
			if (rand.nextDouble() < 1.0 / sample_ratio) {
				index_sampled.add(indexes[i]);
				// System.out.println("Sampled!");
			}
			i = (i + 1) % ms;
		}
		int j = 0;
		for (int k: index_sampled) {
			if (B.get(k)) sketch_sampled.set(j++);
		}
		// System.out.println("Sampled virtual sketch size: " + index_sampled.size() + "\nOriginal virtual sketch size: " + ms);
		return getValue(sketch_sampled);
	}
	
	public int getValue(BitSet b) {
		int zeros = 0;
		int len = b.size();
		for (int i = 0; i < len; i++) {
			if (!b.get(i)) zeros++;
		}
		Double result = -len * Math.log(1.0 * Math.max(zeros, 1)/ len);
		return result.intValue();
	}
	@Override
	public GeneralDataStructure join(GeneralDataStructure gds,int w,int i) {
		GBitmap b = (GBitmap) gds;
		for(int j=0;j<w;j++)
			if((b.getBitmaps().get(i*w+j))){
				B.set(i);
				break;
			}
		return this;
		
	}
	@Override
	public GeneralDataStructure join(GeneralDataStructure gds) {
		GBitmap b = (GBitmap) gds;
		B.or(b.B);
		return this;
	}



	
	
}
