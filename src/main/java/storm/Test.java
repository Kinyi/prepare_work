package storm;

import java.io.File;
import java.util.Collection;

import org.apache.commons.io.FileUtils;

public class Test {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Collection<File> listFiles = FileUtils.listFiles(new File("E://zdir"), new String[]{"txt"}, true);
		for (File file : listFiles) {
			String fileContent = FileUtils.readFileToString(file);
			String[] words = fileContent.split("\\s");
			for (String word : words) {
				if (!"".equals(word)) {
					System.out.println(word);
				}
			}
		}
	}

}
