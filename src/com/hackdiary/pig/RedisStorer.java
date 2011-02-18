package com.hackdiary.pig;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.fs.*;
import org.apache.pig.*;
import org.apache.pig.impl.util.*;
import org.apache.pig.data.Tuple;
import redis.clients.jedis.*;
import java.io.*;
import java.util.*;
import org.apache.commons.lang.StringUtils;

public class RedisStorer extends StoreFunc {
  protected Jedis _jedis = new Jedis("localhost");
  protected RecordWriter _writer = null;

  public RedisStorer() {
  }
  @Override
    public OutputFormat getOutputFormat() {
      return new NullOutputFormat();
    }

  @Override
    public void putNext(Tuple f) throws IOException {
      UDFContext context  = UDFContext.getUDFContext();
      Properties property = context.getUDFProperties(ResourceSchema.class);

      String key = f.get(0).toString();
      List<Object> values = f.getAll();
      String fieldNames = property.getProperty("redis.field.names");
      if(false && fieldNames != null) {
        String[] fields = fieldNames.split(",");
        int idx = 0;
        Pipeline p = _jedis.pipelined();
        for(Object o : values) {
          if(idx != 0 && idx < fields.length && o != null) {
            p.hset(key, fields[idx], o.toString());
          }
          idx++;
        }
        p.execute();
      } else {
        _jedis.set(key,values.get(1).toString());
      }
    }
  @Override
    public void prepareToWrite(RecordWriter writer) {
      this._writer = writer;
    }

  @Override
    public void setStoreLocation(String location, Job job) throws IOException {
      UDFContext context  = UDFContext.getUDFContext();
      Properties property = context.getUDFProperties(ResourceSchema.class);
      property.setProperty("redis.location", location);
    }

  @Override
    public void checkSchema(ResourceSchema s) throws IOException {
      UDFContext context  = UDFContext.getUDFContext();
      Properties property = context.getUDFProperties(ResourceSchema.class);
      String fieldNames   = "";       
      for (String field : s.fieldNames()) {
        fieldNames += field;
        fieldNames += ",";
      }
      property.setProperty("redis.field.names", fieldNames);
    }
}
