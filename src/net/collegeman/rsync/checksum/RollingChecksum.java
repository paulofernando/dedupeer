package net.collegeman.rsync.checksum;

public class RollingChecksum {

	private byte[] data;
	private int blockSize;
	private int index = -1;
	private long a = 0;
	private long b = 0;
	private static double M = Math.pow(2, 16);

	public RollingChecksum(byte[] data, int blockSize) {
		this.data = data;
		this.blockSize = blockSize;
	}

	public final void reset() {
		index = 0;
	}

	public boolean next() {
		if (index < data.length - 1) {
			index++;
			return true;
		} else
			return false;
	}

	public static final Long sum(byte[] chunk) {
		long a = 0;
		for (int i = 0; i < chunk.length; i++)
			a += chunk[i];
		a = (long) (a % M);

		long b = 0;
		for (int i = 0; i < chunk.length; i++)
			b += (chunk.length - 1 - i + 1) * chunk[i];
		b = (long) (b % M);

		return a + (long) (M * b);
	}

	public long weak() {

		if (index == 0) { // s(l, k) = a(l, k) + M * b(l, k)

			byte[] chunk = new byte[Math.min(blockSize, data.length)];

			index = blockSize - 1;

			for (int i = 0; i < chunk.length; i++)
				chunk[i] = data[i];

			for (int i = 0; i < chunk.length; i++)
				a += chunk[i];
			a = (long) (a % M);

			for (int i = 0; i < chunk.length; i++)
				b += (chunk.length - 1 - i + 1) * chunk[i];
			b = (long) (b % M);

			return a + (long) (M * b);

		}

		else {

			int kAt = index - blockSize;
			int lAt = index;

			byte k = data[kAt];
			byte l = data[lAt];

			a = (long) ((a - k + l) % M);
			b = (long) ((b - ((lAt - 1) - kAt + 1) * k + a) % M);

			return a + (long) (M * b);
		}
	}

	public byte[] strong() {
		byte[] chunk = new byte[Math.min(blockSize, data.length - index)];
		return MD5.digest(chunk);
	}

}