package com.hackdiary.pig;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.fs.*;
import org.apache.pig.*;
import org.apache.pig.data.*;
import org.apache.pig.impl.util.*;
import org.apache.pig.data.Tuple;
import redis.clients.jedis.*;
import java.io.*;
import java.util.*;
import org.apache.commons.lang.StringUtils;

public class RedisStorer extends StoreFunc {
  protected Jedis _jedis;
  protected RecordWriter _writer;
  protected String _mode;
  protected String _host;
  protected int _port;

  public RedisStorer() {
    this("kv","localhost",6379);
  }
  public RedisStorer(String mode) {
    this(mode,"localhost",6379);
  }
  public RedisStorer(String mode, String host) {
    this(mode,host,6379);
  }
  public RedisStorer(String mode, String host, int port) {
    _host = host;
    _port = port;
    _mode = mode;
  }

  @Override
    public OutputFormat getOutputFormat() {
      return new NullOutputFormat();
    }

  @Override
    public void putNext(Tuple f) throws IOException {
      if(f.get(0) == null) {
        return;
      }

      String key = f.get(0).toString();
      List<Object> values = f.getAll();
      if(_mode.equals("kv")) {
        if(values.get(1) != null) {
          _jedis.set(key,values.get(1).toString());
        }
      }
      if(_mode.equals("set")) {
        int idx = 0;
        Pipeline p = _jedis.pipelined();
        for(Object o : values) {
          if(idx != 0 && o != null) {
            switch (DataType.findType(o)) {
              case DataType.TUPLE:
              case DataType.BAG:
                for(Object o2 : (Iterable)o) {
                  p.sadd(key, o2.toString());
                }
                break;
              default:
                p.sadd(key, o.toString());
                break;
            }
          }
          idx++;
        }
        p.execute();
      }
      if(_mode.equals("hash")) {
        UDFContext context  = UDFContext.getUDFContext();
        Properties property = context.getUDFProperties(ResourceSchema.class);
        String fieldNames = property.getProperty("redis.field.names");

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
      }
    }

  @Override
    public void prepareToWrite(RecordWriter writer) {
      _writer = writer;
      _jedis = new Jedis(_host,_port);
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
