package adaptiveVirtualBitmapstrict2;

import java.util.BitSet;
import java.util.HashSet;

public class MRBitmap extends GeneralDataStructure{
	/** parameters for bitmap */
	public int bitmapValue;										// the value derived from the bitmap
	public static String name = "MRBitmap";						// bitmap data structure name
	public int arrayLength;								// the size of the bit array
	public int numOfTrue=5;
	public int[] ones;
	public int round;
	public BitSet B;
	public BitSet[] MRB;
	public int T;
	public MRBitmap(int s, int r) {
		super();
		arrayLength = s;
		T = (int)(s*0.935);
		round = r;
		ones = new int[round];
		//round = 1;
		B = new BitSet(s);
		MRB = new BitSet[r];
		for (int i=0;i<r;i++) {
			MRB[i] = new BitSet(s);
		}
		
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
	public int getOnes(BitSet A) {
		int ones=0;
		for (int i=0;i<A.size();i++) {
			if (A.get(i)) ones++;
		}
		return ones;
	}
	@Override
	public int getValue() {
		//System.out.println(numOfTrue+"\t"+round);
		int baseIndex = round-1;
		//System.out.println(round);
		//while(baseIndex>0&&getOnes(MRB[baseIndex])<=T) {
		while(baseIndex>0&&(ones[baseIndex])<=T) {

			baseIndex --;
		}
		int result =0;
		int i = baseIndex;
		while(baseIndex<round) {
			result += -arrayLength * Math.log(1.0 * Math.max(arrayLength-ones[baseIndex], 1)/ arrayLength);//getValue(MRB[baseIndex]);
			baseIndex++;
		}
		
		result *= Math.pow(2, i);
		
		return Math.max((int)(result),1);
		
	}
	@Override
	public void encode() {
		int k = rand.nextInt(arrayLength);
		B.set(k);
	}
	
	@Override
	public void encode(String elementID) {
		int hashValue =Math.abs( GeneralUtil.FNVHash1(elementID));
		int geo_ = Integer.numberOfLeadingZeros(hashValue<<1);
		if (geo_<round) {
			hashValue = Math.abs( GeneralUtil.FNVHash1(elementID+"fkkl"));		
			int k = (hashValue % arrayLength + arrayLength) % arrayLength;
			if  (!MRB[geo_].get(k)) { MRB[geo_].set(k); ones[geo_]+=1;}
		}
	}
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
		GeneralDataStructure[]  B=new MRBitmap[m];
		for(int i=0;i<m;i++) {
			B[i]=new MRBitmap(w,30);
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
		//for (int i = 0; i < len; i++) {
			//if (!b.get(i)) zeros++;
		//}
		
		Double result = -len * Math.log(1.0 * Math.max(zeros, 1)/ len);
		return result.intValue();
	}
	@Override
	public GeneralDataStructure join(GeneralDataStructure gds,int w,int i) {
		MRBitmap b = (MRBitmap) gds;
		for(int j=0;j<w;j++)
			if((b.getBitmaps().get(i*w+j))){
				B.set(i);
				break;
			}
		return this;
		
	}
	@Override
	public GeneralDataStructure join(GeneralDataStructure gds) {
		MRBitmap b = (MRBitmap) gds;
		B.or(b.B);
		return this;
	}



	
	
}
