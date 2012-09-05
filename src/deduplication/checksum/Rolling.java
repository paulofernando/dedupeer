package deduplication.checksum;

public class Rolling {
	public static void rollingTeste(byte[] data) {
		int index = 0;

		long A = 0l;
		for(int i = 0; i < data.length; i++) {
			A += data[i];
		}
		
		long B = 0l;
		for(int i = 0; i < data.length; i++) {
			B += (data.length - i) * data[i];
		}
		
		System.out.println("A = " + A + " e B = " + B);
	}
}

