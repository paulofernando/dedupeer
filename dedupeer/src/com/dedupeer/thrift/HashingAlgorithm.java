/**
 * Autogenerated by Thrift Compiler (0.8.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.dedupeer.thrift;


import java.util.Map;
import java.util.HashMap;
import org.apache.thrift.TEnum;

public enum HashingAlgorithm implements org.apache.thrift.TEnum {
  MD5(1),
  SHA1(2),
  SHA256(3),
  SHA384(4),
  SHA512(5);

  private final int value;

  private HashingAlgorithm(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static HashingAlgorithm findByValue(int value) { 
    switch (value) {
      case 1:
        return MD5;
      case 2:
        return SHA1;
      case 3:
        return SHA256;
      case 4:
        return SHA384;
      case 5:
        return SHA512;
      default:
        return null;
    }
  }
}
