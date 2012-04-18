# RedisStorer

A [UDF StoreFunc](http://pig.apache.org/docs/r0.8.0/udf.html#Store+Functions) for [Apache Pig](http://pig.apache.org/) designed to bulk-load data into [Redis](http://redis.io). Inspired by [wonderdog](https://github.com/infochimps/wonderdog), the Infochimps bulk-loader for elasticsearch.

## Compiling and running

Compile:

Dependencies are automatically retrieved using [Ivy](http://ant.apache.org/ivy/).

    $ ant hadoop

Use:

    $ pig
    grunt> REGISTER dist/pig-redis.jar;
    grunt> a = LOAD 'somefile.tsv' USING PigStorage('\t');
    grunt> STORE a INTO 'dummy-filename-is-ignored' USING com.hackdiary.pig.RedisStorer('kv', 'localhost');

## Bulkloading strategy

RedisStorer runs in three modes: kv, set and hash (specified as the first argument to RedisStorer). If no mode is specified, kv is the default.

In kv mode, it takes the first field of the stored tuple as the key, and the second field as the value, and issues [SET key value](http://redis.io/commands/set). Any further fields are ignored.

In set mode, it takes the first field of the stored tuple as the key, and issues [SADD key value](http://redis.io/commands/sadd) once for each subsequent field value in the tuple.

In list mode, it takes the first field of the stored tuple as the key, and issues [LPUSH key value](http://redis.io/commands/lpush) once for each subsequent field value in the tuple.

In hash mode, it takes the first field of the stored tuple as the key, and issues [HSET key fieldname value](http://redis.io/commands/hset) once for each subsequent field value, using the same key for each, and taking the fieldname from the tuple's schema fieldnames. This means that it will fail unless the stored tuple has a schema with named fields.