import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.EncodingStrategy;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.WriterOptions;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class JdbcOrc {
	
	private static final Charset UTF8 = Charset.forName("UTF-8");
	
	public static void main(String[] args) throws Exception {
		
		String url = System.getenv("JDBC_URL");
		String user = System.getenv("JDBC_USER");
		String password = System.getenv("JDBC_PASSWORD");
		//System.out.println("user:" + user + " password:" + password);
		String query = System.getenv("JDBC_QUERY");

		String out = System.getenv("OUT_FILE");
		String bloomColumns = System.getenv("BLOOM_COLUMNS");

		Connection conn = null;
		if (user != null && password != null) {
			conn = DriverManager.getConnection(url, user, password);
		} else  {
			conn = DriverManager.getConnection(url);
		}
		//conn.createStatement().execute(" CREATE TABLE holder (id int) ");
		
		/*for (int i=0;i<1000;i++) {
			conn.createStatement().execute(" INSERT INTO holder (id) VALUES (1) ");
			conn.createStatement().execute(" INSERT INTO holder (id) VALUES (2) ");
			conn.createStatement().execute(" INSERT INTO holder (id) VALUES (3) ");
			conn.createStatement().execute(" INSERT INTO holder (id) VALUES (4) ");
			conn.createStatement().execute(" INSERT INTO holder (id) VALUES (5) ");
			conn.createStatement().execute(" INSERT INTO holder (id) VALUES (6) ");
			conn.createStatement().execute(" INSERT INTO holder (id) VALUES (7) ");
			conn.createStatement().execute(" INSERT INTO holder (id) VALUES (8) ");
			conn.createStatement().execute(" INSERT INTO holder (id) VALUES (9) ");
			conn.createStatement().execute(" INSERT INTO holder (id) VALUES (10) ");
		}*/
		
		// Query
		Statement stat = conn.createStatement();
		ResultSet rs = stat.executeQuery(query);
		
		// Paths
		Configuration conf = new Configuration();
		FileSystem  fs = FileSystem.getLocal(conf);
		Path  testFilePath = new Path(out);
		fs.delete(testFilePath, false);
		
		// Open a writer
		ObjectInspector inspector = new ResulSetObjectInspector(rs.getMetaData());
		
		WriterOptions options = OrcFile.writerOptions(conf)
                .inspector(inspector)
                .stripeSize(100000)
                .compress(CompressionKind.NONE)
                .bufferSize(10000)
                .encodingStrategy(EncodingStrategy.COMPRESSION);
		if (bloomColumns!=null){
			options.bloomFilterColumns(bloomColumns);
		}
		Writer writer = OrcFile.createWriter(testFilePath, options);

		writer.addUserMetadata("test", UTF8.encode("12"));

		long i = 0;
	    while (rs.next())
		{
	    	writer.addRow(rs);
	    	i++;

	    	/*if ((i%1000)==0) {
	    		//writer.writeIntermediateFooter();
	    		System.out.println("RawDataSize:" + writer.getRawDataSize());
	    		System.out.println("NumberOfRows:" + writer.getNumberOfRows());
	    	}*/
		}
		//writer.writeIntermediateFooter();
		//System.out.println("RawDataSize:" + writer.getRawDataSize());
		//System.out.println("NumberOfRows:" + writer.getNumberOfRows());
	    writer.close();
  }
}
