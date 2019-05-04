package com.neoremind.kafka.examples.utils.id.generator;

import com.google.common.base.Preconditions;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author xu.zx
 */
public class IdGenerator {

  private IdPersister idPersister;

  private AtomicInteger id = new AtomicInteger(0);

  public IdGenerator(IdPersister idPersister) {
    Preconditions.checkNotNull(idPersister);
    this.idPersister = idPersister;
    Integer loadedId = idPersister.load();
    if (loadedId != null && loadedId >= 0) {
      id.set(loadedId);
    }
  }

  public Integer getNext() {
    int result = id.incrementAndGet();
    idPersister.save(result);
    return result;
  }
}
