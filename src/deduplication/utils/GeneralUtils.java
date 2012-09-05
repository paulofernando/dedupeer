package deduplication.utils;

public class GeneralUtils {

	public static String toHex(byte[] data) {
		StringBuilder s = new StringBuilder();
        for (int i = 0; i < data.length; i++) {
            int high = ((data[i] >> 4) & 0xf) << 4;
            int low = data[i] & 0xf;
            if (high == 0) s.append('0');
            s.append(Integer.toHexString(high | low));
        }
        return s.toString();
	}
	
}
