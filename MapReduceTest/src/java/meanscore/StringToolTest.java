package meanscore;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

public class StringToolTest {
	public static void main(String[] args) {
		String fileName = "C:\\Users\\zxb\\git\\zxb\\MapReduceTest\\src\\meanscore\\MeanScoreMapper.java";
		File file = new File(fileName);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String tmpStr;
			int line = 0;
			while ((tmpStr = reader.readLine()) != null) {
				StringTokenizer st = new StringTokenizer(tmpStr);
				while (st.hasMoreTokens()) {
					String tmp = st.nextToken();
					System.out.println(tmp);
				}
				line++;
				System.out.println("----------" + line + "----------------");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}
}
