import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class ResulSetObjectInspector extends SettableStructObjectInspector {

	public ResulSetObjectInspector(ResultSetMetaData metaData) throws SQLException {
		init(metaData, null, null);
	}

	/**
	   * MyField.
	   *
	   */
	  public static class MyField implements StructField {
	    protected int fieldID;
	    protected String name;

	    protected ObjectInspector fieldObjectInspector;

	    protected MyField() {
	      super();
	    }

	    public MyField(int fieldID, String name, ObjectInspector fieldObjectInspector) {
	      this.fieldID = fieldID;
	      this.name = name;
	      this.fieldObjectInspector = fieldObjectInspector;
	    }

	    public String getFieldName() {
	      return name.toLowerCase();
	    }

	    public ObjectInspector getFieldObjectInspector() {
	      return fieldObjectInspector;
	    }

	    public int getFieldID() {
	      return fieldID;
	    }

	    public String getFieldComment() {
	      return null;
	    }

	    @Override
	    public String toString() {
	      return name;
	    }
	  }

	  //Class<?> objectClass;
	  List<MyField> fields;
	  volatile boolean inited = false;
	  //volatile Type type;

	  public Category getCategory() {
	    return Category.STRUCT;
	  }

	  public String getTypeName() {
	    StringBuilder sb = new StringBuilder("struct<");
	    boolean first = true;
	    for (StructField structField : getAllStructFieldRefs()) {
	      if (first) {
	        first = false;
	      } else {
	        sb.append(",");
	      }
	      sb.append(structField.getFieldName()).append(":")
	          .append(structField.getFieldObjectInspector().getTypeName());
	    }
	    sb.append(">");
	    return sb.toString();
	  }



	  /**
	   * This method is only intended to be used by Utilities class in this package.
	   * The reason that this method is not recursive by itself is because we want
	   * to allow recursive types.
	 * @throws SQLException 
	   */
	  protected void init(ResultSetMetaData metadata, Class<?> objectClass,
	      ObjectInspectorFactory.ObjectInspectorOptions options) throws SQLException {
	    //this.type = type;

	    verifyObjectClassType(objectClass);
	    //this.objectClass = objectClass;
	    final List<? extends ObjectInspector> structFieldObjectInspectors = extractFieldObjectInspectors(metadata);

	    synchronized (this) {
	      fields = new ArrayList<MyField>(structFieldObjectInspectors.size());
	      int used = 0;
	      for (int i = 0; i < metadata.getColumnCount(); i++) {
	        if (!shouldIgnoreField(metadata.getColumnName(i+1))) {
	          fields.add(new MyField(i, metadata.getColumnName(i+1), structFieldObjectInspectors
	              .get(used++)));
	        }
	      }
	      assert (fields.size() == structFieldObjectInspectors.size());
	      inited = true;
	      notifyAll();
	    }
	  }

	  // ThriftStructObjectInspector will override and ignore __isset fields.
	  public boolean shouldIgnoreField(String name) {
	    return false;
	  }

	  // Without Data
	  @Override
	  public StructField getStructFieldRef(String fieldName) {
	    return ObjectInspectorUtils.getStandardStructFieldRef(fieldName, fields);
	  }

	  @Override
	  public List<? extends StructField> getAllStructFieldRefs() {
	    return fields;
	  }

	  // With Data
	  @Override
	  public Object getStructFieldData(Object data, StructField fieldRef) {
	    if (data == null) {
	      return null;
	    }
	    if (!(fieldRef instanceof MyField)) {
	      throw new RuntimeException("fieldRef has to be of MyField");
	    }
	    MyField f = (MyField) fieldRef;
	    try {
	    	//Object r = f.field.get(data);
	    	ResultSet rs = (ResultSet)data;
	    	Object r = rs.getObject(f.fieldID+1);
	      return r;
	    } catch (Exception e) {
	      throw new RuntimeException("cannot get field " + f.name + " from "
	          + data.getClass() + " " + data, e);
	    }
	  }

	  @Override
	  public List<Object> getStructFieldsDataAsList(Object data) {
	    if (data == null) {
	      return null;
	    }
	    try {
	      ArrayList<Object> result = new ArrayList<Object>(fields.size());
	      for (int i = 0; i < fields.size(); i++) {
	        //result.add(fields.get(i).field.get(data));

		    	int a = 10/0;
	      }
	      return result;
	    } catch (Exception e) {
	      throw new RuntimeException(e);
	    }
	  }

	  @Override
	  public Object create() {
		  // HACK
	    //return ReflectionUtils.newInstance(objectClass, null);
		  return new HashMap<String,Object>();
	  }

	  @Override
	  public Object setStructFieldData(Object struct, StructField field,
	      Object fieldValue) {
	    MyField myField = (MyField) field;
	    try {
	      //myField.field.set(struct, fieldValue);
	    	int a = 10/0;
	    } catch (Exception e) {
	      throw new RuntimeException("cannot set field " + myField.name + " of "
	          + struct.getClass() + " " + struct, e);
	    }
	    return struct;
	  }

	  protected List<? extends ObjectInspector> extractFieldObjectInspectors(ResultSetMetaData metaData) throws SQLException {
	    ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>(metaData.getColumnCount());
	    for (int i = 1; i <= metaData.getColumnCount(); i++) {
	      if (!shouldIgnoreField(metaData.getColumnName(i))) {
	        structFieldObjectInspectors.add(translateFromJdbc(metaData.getColumnType(i)));
	      }
	    }
	    return structFieldObjectInspectors;
	  }


	  private ObjectInspector translateFromJdbc(int columnType) throws SQLException {
		switch (columnType) {
		case Types.INTEGER:
			return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
		case Types.BIGINT:
			return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
		case Types.DOUBLE:
			return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
		case Types.BOOLEAN:
			return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
		case Types.DECIMAL:
			return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		case Types.VARCHAR:
		case Types.NVARCHAR:
			return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		case Types.CHAR:
			return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		case Types.DATE:
			return PrimitiveObjectInspectorFactory.javaDateObjectInspector;
		case Types.TIMESTAMP:
			return PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
		case Types.BIT:
			return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
			
		case Types.BINARY:
			return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
			
		default:
			throw new SQLException("columnType{" + columnType + "} not implemented");
		}
	}

	protected void verifyObjectClassType(Class<?> objectClass) {
	    assert (!List.class.isAssignableFrom(objectClass));
	    assert (!Map.class.isAssignableFrom(objectClass));
	  }
	

}
