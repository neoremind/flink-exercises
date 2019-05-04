package com.neoremind.kafka.examples.utils.id.generator;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import lombok.extern.slf4j.Slf4j;

/**
 * @author xu.zx
 */
@Slf4j
public class FileSystemIdPersister implements IdPersister {

  private String filePath;

  public FileSystemIdPersister(String filePath) {
    Preconditions.checkNotNull(filePath);
    this.filePath = filePath;
    File f = new File(filePath);
    if (!f.exists()) {
      try {
        f.createNewFile();
      } catch (IOException e) {
        Throwables.propagate(e);
      }
    }
  }

  @Override
  public Integer load() {
    try {
      String content = Files.readFirstLine(new File(filePath), Charset.defaultCharset());
      return Integer.parseInt(content);
    } catch (NumberFormatException e) {
      return 0;
    } catch (Exception e) {
      Throwables.propagate(e);
      return 0;
    }
  }

  @Override
  public void save(Integer id) {
    try {
      Files.write(String.valueOf(id).getBytes(), new File(filePath));
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

}
