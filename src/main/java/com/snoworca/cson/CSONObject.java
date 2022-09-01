package com.snoworca.cson;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

public class CSONObject extends CSONElement {
	
	private LinkedHashMap<String, Object> mTree = new LinkedHashMap<>();
	
	
	public static CSONObject parse(byte[] buffer) {		
		return (CSONObject)CSONParser.parse(buffer);
	}
	
	
	public CSONObject() {
		super(ElementType.Object);
	}
	
	public CSONObject put(String key, Object value) {
		if(value == null) {
			mTree.put(key, new NullValue());
			return this;
		}
		else if(value instanceof Number) {
			mTree.put(key, value);
		} else if(value instanceof String) {
			mTree.put(key, value);
		} else if(value instanceof CharSequence) {
			mTree.put(key, value);
		} else if(value instanceof Character || value instanceof Boolean || value instanceof CSONElement || value instanceof byte[] || value instanceof NullValue) {
			mTree.put(key, value);
		}
		return this;
	}
	
	
	public String optString(String key) {
		Object obj = mTree.get(key);
		return DataConverter.toString(obj);
	}
	
	public String optString(String key, String def) {
		Object obj = mTree.get(key);
		if(obj == null) return def;
		return DataConverter.toString(obj);
	}
	
	public String getString(String key) {
		Object obj = mTree.get(key);
		if(obj == null) throw new CSONIndexNotFoundException();
		return DataConverter.toString(obj);
	}
	
	public Object opt(String key) {
		Object obj = mTree.get(key);
		if(obj instanceof NullValue) return null;
		return obj;
	}
	
	public Object get(String i) {
		Object obj =  mTree.get(i);
		if(obj instanceof NullValue) return null;
		else if(obj == null) throw new CSONIndexNotFoundException();
		return obj;
		
	}
	
	public Object opt(String key, Object def) {
		Object result = mTree.get(key);
		if(result instanceof NullValue) return null;
		else if(result == null) return def; 
		return result;
	}
	
	public int optInteger(String key, int def) {
 		Object obj = mTree.get(key);
 		if(obj == null) return def;
		return DataConverter.toInteger(obj);
	}
	
	public int optInteger(String key) {
 		return optInteger(key, 0);
	}
	
	public int getInteger(String key) {
 		Object obj = mTree.get(key); 
 		if(obj == null) throw new CSONIndexNotFoundException();
		return DataConverter.toInteger(obj);
	}
	
	public float optFloat(String key) {
		return optFloat(key, Float.NaN);
	}
	
	public float optFloat(String key, float def) {
		Object obj = mTree.get(key); 
 		if(obj == null) return def;
		return DataConverter.toFloat(obj);
	}
	
	public float getFloat(String key) {
		Object obj = mTree.get(key); 
		if(obj == null) throw new CSONIndexNotFoundException();
		return DataConverter.toFloat(obj);
	}
	
	public boolean optBoolean(String key) { 
		return optBoolean(key, false);
	}
	
	public boolean optBoolean(String key, boolean def) { 
		Object obj = mTree.get(key); 
		if(obj  == null) return def;
		return DataConverter.toBoolean(obj);
	}
	
	public boolean getBoolean(String key) { 
		Object obj = mTree.get(key); 
		if(obj == null) throw new CSONIndexNotFoundException();
		return DataConverter.toBoolean(obj);
	}
	
	public long getLong(String key) { 
		Object obj = mTree.get(key); 
		if(obj == null) throw new CSONIndexNotFoundException();
		return DataConverter.toLong(obj);
	}
	
	public double getDouble(String key) { 
		Object obj = mTree.get(key); 
		if(obj == null) throw new CSONIndexNotFoundException();
		return DataConverter.toDouble(obj);
	}
	
	public char getChar(String key) { 
		Object obj = mTree.get(key); 
		if(obj == null) throw new CSONIndexNotFoundException();
		return DataConverter.toChar(obj);
	}
	
	
	public CSONArray optArray(String key) {
		Object obj = mTree.get(key);
		if(obj instanceof CSONArray) {
			return (CSONArray)obj;
		}
		return null;
	}
	
	
	public CSONArray getArray(String key) {
		Object obj = mTree.get(key);
		if(obj instanceof CSONArray) {
			return (CSONArray)obj;
		}
		throw new CSONIndexNotFoundException();
	}
	
	public CSONObject optObject(String key) {
		Object obj = mTree.get(key);
		if(obj instanceof CSONObject) {
			return (CSONObject)obj;
		}
		return null;
	}
	
	public CSONObject getObject(String key) {
		Object obj = mTree.get(key);
		if(obj instanceof CSONObject) {
			return (CSONObject)obj;
		}
		throw new CSONIndexNotFoundException();
	}
	
	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		writeJSONString(stringBuilder);
		return stringBuilder.toString();
	}
	
	public byte[] toByteArray() {
		CSONWriter writer = new CSONWriter();
		write(writer);
		return writer.toByteArray();
	}
	
	protected void write(CSONWriter writer) {
		Iterator<Entry<String, Object>> iter = mTree.entrySet().iterator();
		writer.openObject();
		while(iter.hasNext()) {
			Entry<String, Object> entry = iter.next();
			String key = entry.getKey();
			Object obj = entry.getValue();
			if(obj == null || obj instanceof NullValue) writer.key(key).nullValue();
			else if(obj instanceof CSONArray)  {
				writer.key(key);
				((CSONArray)obj).write(writer);
			}
			else if(obj instanceof CSONObject)  {
				writer.key(key);
				((CSONObject)obj).write(writer);
			} 
			else if(obj instanceof Byte)	writer.key(key).value((Byte)obj);
			else if(obj instanceof Short)	writer.key(key).value((Short)obj);
			else if(obj instanceof Character) writer.key(key).value((Character)obj);
			else if(obj instanceof Integer) writer.key(key).value((Integer)obj);
			else if(obj instanceof Float) writer.key(key).value((Float)obj);
			else if(obj instanceof Long) writer.key(key).value((Long)obj);
			else if(obj instanceof Double) writer.key(key).value((Double)obj);
			else if(obj instanceof String) writer.key(key).value((String)obj);
			else if(obj instanceof Boolean) writer.key(key).value((Boolean)obj);
			else if(obj instanceof byte[]) writer.key(key).value((byte[])obj);
		}
		writer.closeObject();
	}
	
	protected void writeJSONString(StringBuilder strBuilder) {
		
		strBuilder.append("{");
		
		Iterator<Entry<String, Object>> iter = mTree.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<String, Object> entry = iter.next();
			String key = entry.getKey();
			Object obj = entry.getValue();
			strBuilder.append('"').append(key).append("\":");
			if(obj == null || obj instanceof NullValue) strBuilder.append("null");
			else if(obj instanceof Number || obj instanceof Boolean) strBuilder.append(obj);
			else if(obj instanceof Character) strBuilder.append('"').append(obj).append('"');
			else if(obj instanceof String) strBuilder.append('"').append(DataConverter.escapeJSONString((String)obj)).append('"');
			else if(obj instanceof byte[]) strBuilder.append('"').append(DataConverter.toString(obj)).append('"');
			else if(obj instanceof CSONArray) ((CSONArray)obj).writeJSONString(strBuilder);
			else if(obj instanceof CSONObject) ((CSONObject)obj).writeJSONString(strBuilder);
			
			if(iter.hasNext()) strBuilder.append(',');
			
		}		
		strBuilder.append("}");
	}
	
	
	
}
