import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.ColumnStatistics;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.StripeInformation;

public class ReadOrc 
{
	private static final Charset UTF8 = Charset.forName("UTF-8");

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		FileSystem  fs = FileSystem.getLocal(conf);
		Path  testFilePath = new Path("temp.orc");
	    System.out.println("Path: " + testFilePath);

	    Reader reader = OrcFile.createReader(testFilePath,
		        OrcFile.readerOptions(conf).filesystem(fs));
	    
	    printInfo(reader);
		    /*RecordReader rows = reader.rows();
		    int i = 0;
		    while (rows.hasNext() && i++ < 10) {
		      Object row = rows.next(null);
		      //assertEquals(new IntWritable(111), ((OrcStruct) row).getFieldValue(0));
		      //assertEquals(new LongWritable(1111), ((OrcStruct) row).getFieldValue(1));
		      System.out.println("Row: " + row);
		    }*/
	}

	private static void printInfo(Reader reader) {
		System.out.println("FileVersion:" + reader.getFileVersion());
		System.out.println("ContentLength:" + reader.getContentLength() + " (real size of the file - size of the footer)");
		System.out.println("CompressionSize:" + reader.getCompressionSize());
		System.out.println("Compression:" + reader.getCompression());
		System.out.println("RawDataSize:" + reader.getRawDataSize());
		System.out.println("NumberOfRows:" + reader.getNumberOfRows());
		for (String key : reader.getMetadataKeys()) {
			System.out.println("MetadataKey[" + key + "]:" + UTF8.decode(reader.getMetadataValue(key)));
		}
		for (StripeInformation stripe : reader.getStripes()){
			System.out.println("StripeInformation:" + stripe);
		}
		for (ColumnStatistics stats : reader.getStatistics()){
			System.out.println("ColumnStatistics:" + stats);
		}
		//for (dd dd : reader.)
	}
}
