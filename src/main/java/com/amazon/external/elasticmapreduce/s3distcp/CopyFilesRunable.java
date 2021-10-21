package com.amazon.external.elasticmapreduce.s3distcp;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.common.Abortable;

//import amazon.emr.metrics.MetricsSaver;
//import amazon.emr.metrics.MetricsSaver.StopWatch;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.ObjectMapper;

class CopyFilesRunable implements Runnable {
  private static final Log LOG = LogFactory.getLog(CopyFilesRunable.class);
  private final List<FileInfo> fileInfos;
  private final CopyFilesReducer reducer;
  private final String tempPath;
  private final Path finalPath;
  private final boolean groupWithNewLine;
  private final boolean jsonFileValidation;
  private final Charset UTF_8 = StandardCharsets.UTF_8;
  private final byte[] newLine = "\n".getBytes(UTF_8);
  private final ObjectMapper objectMapper;

  public CopyFilesRunable(CopyFilesReducer reducer, List<FileInfo> fileInfos, Path tempPath, Path finalPath,
      boolean groupWithNewLine, boolean jsonFileValidation) {
    this.fileInfos = fileInfos;
    this.reducer = reducer;
    this.tempPath = tempPath.toString();
    this.finalPath = finalPath;
    this.groupWithNewLine = groupWithNewLine;
    this.jsonFileValidation = jsonFileValidation;

    this.objectMapper = new ObjectMapper();
    objectMapper.configure(Feature.ALLOW_SINGLE_QUOTES, true);
    objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);

    LOG.info("Creating CopyFilesRunnable " + tempPath.toString() + ":" + finalPath.toString());
  }

  private long copyStream1(final InputStream inputStream, OutputStream outputStream, MessageDigest md,
      final boolean isLastFile) throws IOException {
    long bytesCopied = 0L;

    final InputStream inputStreamLocal;
    if (this.jsonFileValidation) {
      try {
        Map<?, ?> map = this.objectMapper.readValue(inputStream, Map.class);
        inputStreamLocal = new ByteArrayInputStream(objectMapper.writeValueAsBytes(map));
      } catch (IOException e) {
        LOG.warn("Error json parsing or mapping ", e);
        return 0L;
      }
    } else {
      inputStreamLocal = inputStream;
    }

    int len = 0;
    int lastLen = 0;
    byte[] buffer = new byte[this.reducer.getBufferSize()];
    while ((len = inputStreamLocal.read(buffer)) > 0) {
      md.update(buffer, 0, len);
      outputStream.write(buffer, 0, len);
      this.reducer.progress();
      bytesCopied += len;
      lastLen = len;
    }

    if (this.groupWithNewLine && !isLastFile && buffer[lastLen - 1] != newLine[0]) {
      // 最後が改行でなければ改行を追記
      md.update(newLine, 0, 1);
      outputStream.write(newLine, 0, 1);
      this.reducer.progress();
      bytesCopied += 1;
    }

    return bytesCopied;
  }

  private long copyStream2(InputStream inputStream, OutputStream outputStream, MessageDigest md) throws IOException {
    long bytesCopied = 0L;

    int len = 0;
    byte[] buffer = new byte[this.reducer.getBufferSize()];

    while ((len = inputStream.read(buffer)) > 0) {
      md.update(buffer, 0, len);
      outputStream.write(buffer, 0, len);
      this.reducer.progress();
      bytesCopied += len;
    }

    return bytesCopied;
  }

  private ProcessedFile downloadAndMergeInputFiles() throws IOException {
    int numRetriesRemaining = this.reducer.getNumTransferRetries();

    boolean finished = false;
    while ((!finished) && (numRetriesRemaining > 0)) {
      numRetriesRemaining--;
      final Path curTempPath = new Path(this.tempPath + UUID.randomUUID());

      try (OutputStream outputStream = this.reducer.openOutputStream(curTempPath)) {
        LOG.info("Opening temp file: " + curTempPath.toString());
        MessageDigest md = MessageDigest.getInstance("MD5");

        for (Iterator<FileInfo> it = this.fileInfos.iterator(); it.hasNext();) {
          final FileInfo fileInfo = it.next();
          final boolean isLastFile = !it.hasNext();
          final Path filePath = new Path(fileInfo.inputFileName.toString());

          try (InputStream inputStream = this.reducer.openInputStream(filePath)) {
            LOG.info("Starting download of " + fileInfo.inputFileName + " to " + curTempPath);
            copyStream1(inputStream, outputStream, md, isLastFile);
          } catch (IOException e) {
            if ((outputStream != null) && ((outputStream instanceof Abortable))) {
              LOG.warn("Output stream is abortable, aborting the output stream for " + fileInfo.inputFileName);
              Abortable abortable = (Abortable) outputStream;
              abortable.abort();
            }
            throw e;
          }

          finished = true;
          LOG.info("Finished downloading " + fileInfo.inputFileName);
        }

        return new ProcessedFile(md.digest(), curTempPath);

      } catch (IOException e) {
        LOG.warn("Exception raised while copying file data to:  file=" + this.finalPath + " numRetriesRemaining="
            + numRetriesRemaining, e);
        try {
          FileSystem fs = curTempPath.getFileSystem(this.reducer.getConf());
          fs.delete(curTempPath, false);
        } catch (IOException ignore) {
        }
        if (numRetriesRemaining <= 0) {
          throw e;
        }
      } catch (NoSuchAlgorithmException ignore) {
      }
    }
    return null;
  }

  private static File[] getTempDirs(Configuration conf) {
    String[] backupDirs = conf.get("fs.s3.buffer.dir").split(",");
    List<File> tempDirs = new ArrayList<>(backupDirs.length);
    int directoryIndex = 0;

    File result = null;
    while (directoryIndex < backupDirs.length) {
      File dir = new File(backupDirs[directoryIndex]);
      dir.mkdirs();
      try {
        result = File.createTempFile("output-", ".tmp", dir);
        if (result != null) {
          tempDirs.add(new File(backupDirs[directoryIndex]));
        }
        result.delete();
      } catch (IOException ignore) {
      }
      directoryIndex += 1;
    }

    return (File[]) tempDirs.toArray(new File[0]);
  }

  public void run() {
    int retriesRemaining = this.reducer.getNumTransferRetries();
    ProcessedFile processedFile = null;
    try {
      processedFile = downloadAndMergeInputFiles();
    } catch (IOException e) {
      LOG.warn("Error download input files. Not marking as committed", e);
    }

    while (retriesRemaining > 0) {
      retriesRemaining--;
      try {
        final Path curTempPath = processedFile.path;
        final FileSystem inFs = curTempPath.getFileSystem(this.reducer.getConf());
        final FileSystem outFs = this.finalPath.getFileSystem(this.reducer.getConf());

        if (inFs.getUri().equals(outFs.getUri())) {
          LOG.info("Renaming " + curTempPath.toString() + " to " + this.finalPath.toString());
          inFs.mkdirs(this.finalPath.getParent());
          inFs.rename(curTempPath, this.finalPath);

        } else {
          LOG.info("inFs.getUri()!=outFs.getUri(): " + inFs.getUri() + "!=" + outFs.getUri());

          if (Utils.isS3Scheme(outFs.getUri().getScheme())) {
            byte[] digest = processedFile.checksum;
            copyToS3FinalDestination(curTempPath, inFs, digest);
          } else {
            copyToFinalDestination(curTempPath);
          }
        }

        for (FileInfo fileInfo : this.fileInfos) {
          this.reducer.markFileAsCommited(fileInfo);
          if (this.reducer.shouldDeleteOnSuccess()) {
            LOG.info("Deleting " + fileInfo.inputFileName);
            Path inPath = new Path(fileInfo.inputFileName.toString());
            FileSystem deleteFs = FileSystem.get(inPath.toUri(), this.reducer.getConf());
            deleteFs.delete(inPath, false);
          }
        }

        Path localTempPath = new Path(this.tempPath);
        FileSystem fs = localTempPath.getFileSystem(this.reducer.getConf());
        fs.delete(localTempPath, true);
        return;
      } catch (IOException e) {
        LOG.warn("Error processing files. Not marking as committed", e);
      } catch (Exception e) {
        LOG.warn("Error processing files. Not marking as committed", e);
      }
    }
  }

  private void copyToS3FinalDestination(Path curTempPath, FileSystem inFs, byte[] digest) throws IOException {

    final String bucket = this.finalPath.toUri().getHost();
    final String key = this.finalPath.toUri().getPath().substring(1);
    final FileStatus status = inFs.getFileStatus(curTempPath);

    final AmazonS3Client s3 = S3DistCp.createAmazonS3Client(this.reducer.getConf());
    s3.setEndpoint(this.reducer.getConf().get("fs.s3n.endpoint", "s3.amazonaws.com"));
    final ObjectMetadata meta = new ObjectMetadata();
    meta.setContentLength(status.getLen());
    if (digest != null) {
      meta.setContentMD5(new String(Base64.encodeBase64(digest), UTF_8));
    }

    if (this.reducer.shouldUseMutlipartUpload()) {
      int chunkSize = this.reducer.getMultipartSize();
      try (InputStream inStream = this.reducer.openInputStream(curTempPath);
          OutputStream outStream = new MultipartUploadOutputStream(s3, Utils.createDefaultExecutorService(),
              this.reducer.getProgressable(), bucket, key, meta, chunkSize, getTempDirs(this.reducer.getConf()))) {
        MessageDigest md = MessageDigest.getInstance("MD5");
        copyStream2(inStream, outStream, md);

      } catch (NoSuchAlgorithmException ignore) {
      }
    } else {
      int retries = this.reducer.getNumTransferRetries();
      while (retries > 0) {
        retries--;
        try (InputStream inputStream = this.reducer.openInputStream(curTempPath)) {
          s3.putObject(bucket, finalPath.toUri().getPath(), inputStream, meta);
        }
      }
    }
  }

  private void copyToFinalDestination(Path curTempPath) throws IOException {
    LOG.info("Copying " + curTempPath.toString() + " to " + this.finalPath.toString());

    try (InputStream inStream = this.reducer.openInputStream(curTempPath);
        OutputStream outStream = this.reducer.openOutputStream(this.finalPath)) {
      MessageDigest md = MessageDigest.getInstance("MD5");
      copyStream2(inStream, outStream, md);
    } catch (NoSuchAlgorithmException ignore) {
    }
  }

  private class ProcessedFile {
    public byte[] checksum;
    public Path path;

    public ProcessedFile(byte[] checksum, Path path) {
      this.checksum = checksum;
      this.path = path;
    }
  }
}

/*
 * Location: /Users/libinpan/Work/s3/s3distcp.jar Qualified Name:
 * com.amazon.external.elasticmapreduce.s3distcp.CopyFilesRunable JD-Core
 * Version: 0.6.2
 */