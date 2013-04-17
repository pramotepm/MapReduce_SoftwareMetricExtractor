import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;

public class MyConfigure {
	public String GIT_DIR = null,
				  RELEASE_DATE_FILE = null,
				  RELEASE_REPOSITY_HOME = null;
	
	public MyConfigure(String conf_path) {
		getConf(conf_path);
	}
	
	private void getConf(String path) {
		Properties prop = new Properties();
		try {
			prop.loadFromXML(new FileInputStream(path));
			
			GIT_DIR = prop.getProperty("conf.git.dir");
			RELEASE_DATE_FILE = prop.getProperty("release.date.file");
			RELEASE_REPOSITY_HOME = prop.getProperty("release.repository.home");
			
		} catch (InvalidPropertiesFormatException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
