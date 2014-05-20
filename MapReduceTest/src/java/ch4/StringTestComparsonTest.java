package ch4;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class StringTestComparsonTest {
	public static void main(String[] args) {
		Text t = new Text("\u0041\u00DF\u6771\uD801\uDC00");
		ByteBuffer buf = ByteBuffer.wrap(t.getBytes(), 0, t.getLength());
		int cp ;
		while(buf.hasRemaining() && (cp = Text.bytesToCodePoint(buf)) != -1){
			System.out.println(Integer.toHexString(cp));
		}
	}

	@Test
	public void string() throws UnsupportedEncodingException {
		String s = "\u0041\u00DF\u6771\uD801\uDC00";

		Assert.assertEquals(s.length(), 5);

		Assert.assertEquals(s.getBytes("UTF-8").length, 10);
		Assert.assertEquals(s.indexOf("\u0041"), 0);
		Assert.assertEquals(s.indexOf("\u00DF"), 1);
		Assert.assertEquals(s.indexOf("\u6771"), 2);
		Assert.assertEquals(s.indexOf("\uD801\uDC00"), 3);
		Assert.assertEquals(s.charAt(0), '\u0041');
		Assert.assertEquals(s.charAt(1), '\u00DF');
		Assert.assertEquals(s.charAt(2), '\u6771');
		Assert.assertEquals(s.charAt(3), '\uD801');
		Assert.assertEquals(s.charAt(4), '\uDC00');
		Assert.assertEquals(s.codePointAt(0), 0x0041);
		Assert.assertEquals(s.codePointAt(1), 0x00DF);
		Assert.assertEquals(s.codePointAt(2), 0x6771);
	}

	@Test
	public void text() throws UnsupportedEncodingException {
		Text s = new Text("\u0041\u00DF\u6771\uD801\uDC00");

		Assert.assertEquals(s.getLength(), 10);

		Assert.assertEquals(s.find("\u0041"), 0);
		Assert.assertEquals(s.find("\u00DF"), 1);
		Assert.assertEquals(s.find("\u6771"), 3);
		Assert.assertEquals(s.find("\uD801\uDC00"), 6);
		Assert.assertEquals(s.charAt(0), 0x0041);
		Assert.assertEquals(s.charAt(1), 0x00DF);
		Assert.assertEquals(s.charAt(3), 0x6771);
		Assert.assertEquals(s.charAt(6), 0x10400);
	}
}
