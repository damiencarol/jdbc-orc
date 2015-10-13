import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.EncodingStrategy;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

public class Test {

	public static class Row {
		Integer int1;
		Long long1;

		public Row(int val, long l) {
			this.int1 = val;
			this.long1 = l;
		}
	}

	public static void main(String[] args) throws Exception {

		Path workDir = new Path(
				System.getProperty("test.tmp.dir", "target" + File.separator + "test" + File.separator + "tmp"));

		Configuration conf;
		FileSystem fs;
		Path testFilePath;
		conf = new Configuration();
		fs = FileSystem.getLocal(conf);
		testFilePath = new Path(workDir, "TestOrcFile." + "testCaseName.getMethodName" + ".orc");
		fs.delete(testFilePath, false);

		ObjectInspector inspector;
		inspector = ObjectInspectorFactory.getReflectionObjectInspector(Row.class,
				ObjectInspectorFactory.ObjectInspectorOptions.JAVA);

		Writer writer = OrcFile.createWriter(testFilePath,
				OrcFile.writerOptions(conf).inspector(inspector).stripeSize(100000).compress(CompressionKind.SNAPPY)
						.bufferSize(10000).encodingStrategy(EncodingStrategy.SPEED));
		writer.addRow(new Row(111, 1111L));
		writer.addRow(new Row(111, 1111L));
		writer.addRow(new Row(111, 1111L));
		writer.addRow(new Row(111, 1111L));
		writer.addRow(new Row(111, 1111L));
		writer.addRow(new Row(111, 1111L));
		writer.addRow(new Row(111, 1111L));
		writer.addRow(new Row(111, 1111L));
		writer.addRow(new Row(111, 1111L));
		writer.addRow(new Row(111, 1111L));
		writer.addRow(new Row(111, 1111L));
		writer.addRow(new Row(111, 1111L));
		writer.addRow(new Row(111, 1111L));
		writer.close();

		System.out.println("Path: " + testFilePath);

		Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(fs));
		printInfo(reader);
		RecordReader rows = reader.rows();
		int i = 0;
		while (rows.hasNext() && i++ < 10) {
			Object row = rows.next(null);
			/*
			 * assertEquals(new IntWritable(111), ((OrcStruct)
			 * row).getFieldValue(0)); assertEquals(new LongWritable(1111),
			 * ((OrcStruct) row).getFieldValue(1));
			 */

			System.out.println("Row: " + row);
		}

	}

	private static void printInfo(Reader reader) {
		System.out.println("CompressionSize:" + reader.getCompressionSize());
		System.out.println("NumberOfRows:" + reader.getNumberOfRows());
	}
}
