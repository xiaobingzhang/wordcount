package ch5;

// cc ConfigurationPrinter An example Tool implementation for printing the properties in a Configuration
import java.util.Map.Entry;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

// vv ConfigurationPrinter
public class ConfigurationPrinter extends Configured implements Tool {
  
  static {
    Configuration.addDefaultResource("core-site.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    for (Entry<String, String> entry: conf) {
      System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
    }
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new ConfigurationPrinter(), args);
    System.exit(exitCode);
  }
}
// ^^ ConfigurationPrinter
