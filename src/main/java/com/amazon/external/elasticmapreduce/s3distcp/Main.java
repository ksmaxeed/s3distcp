package com.amazon.external.elasticmapreduce.s3distcp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ToolRunner;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.util.Arrays;

public class Main {
  private static final Log log = LogFactory.getLog(S3DistCp.class);

  public static void main(String[] args) throws Exception {
    log.info("Running with args: " + Arrays.toString(args));

//    String a = "s3://test/testfile";
//    StringBuilder sb = new StringBuilder(100000);
//    for (int i = 0; i < 150000000; i++) {
//      sb.append(a).append(":").append("¥n");
//    }
//    ByteArrayOutputStream ba = new ByteArrayOutputStream();
//    ba.write(sb.toString().getBytes("utf-8"));
//    FileOutputStream fo = new FileOutputStream("/Users/Nine/file");
//    fo.write(sb.toString().getBytes("utf-8"));
     System.exit(ToolRunner.run(new S3DistCp(), args));
  }
}

/*
 * Location: /Users/libinpan/Work/s3/s3distcp.jar Qualified Name:
 * com.amazon.external.elasticmapreduce.s3distcp.Main JD-Core Version: 0.6.2
 */