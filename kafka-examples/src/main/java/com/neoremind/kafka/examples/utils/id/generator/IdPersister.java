package com.neoremind.kafka.examples.utils.id.generator;

/**
 * @author xu.zx
 */
public interface IdPersister {

  Integer load();

  void save(Integer id);

}
