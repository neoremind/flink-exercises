package com.neoremind.flink.exercises.common.model;

import java.io.Serializable;

/**
 * @author xu.zx
 */
public class Word implements Serializable {

  private String text;

  private boolean invalid;

  private long createTimMs;

  public Word(String text) {
    this.text = text;
  }

  public Word(String text, boolean invalid) {
    this.text = text;
    this.invalid = invalid;
  }

  public Word(String text, long createTimMs) {
    this.text = text;
    this.createTimMs = createTimMs;
  }

  public Word(String text, boolean invalid, long createTimMs) {
    this.text = text;
    this.invalid = invalid;
    this.createTimMs = createTimMs;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

  public boolean isInvalid() {
    return invalid;
  }

  public void setInvalid(boolean invalid) {
    this.invalid = invalid;
  }

  public long getCreateTimMs() {
    return createTimMs;
  }

  public void setCreateTimMs(long createTimMs) {
    this.createTimMs = createTimMs;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("Word{");
    sb.append("text='").append(text).append('\'');
    sb.append(", invalid=").append(invalid);
    sb.append(", createTimMs=").append(createTimMs);
    sb.append('}');
    return sb.toString();
  }
}
