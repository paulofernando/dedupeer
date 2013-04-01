/**
 * Autogenerated by Thrift Compiler (0.8.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.dedupeer.thrift;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChunkIDs implements org.apache.thrift.TBase<ChunkIDs, ChunkIDs._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ChunkIDs");

  private static final org.apache.thrift.protocol.TField FILE_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("fileID", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField CHUNK_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("chunkID", org.apache.thrift.protocol.TType.STRING, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new ChunkIDsStandardSchemeFactory());
    schemes.put(TupleScheme.class, new ChunkIDsTupleSchemeFactory());
  }

  public String fileID; // optional
  public String chunkID; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    FILE_ID((short)1, "fileID"),
    CHUNK_ID((short)2, "chunkID");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // FILE_ID
          return FILE_ID;
        case 2: // CHUNK_ID
          return CHUNK_ID;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private _Fields optionals[] = {_Fields.FILE_ID};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.FILE_ID, new org.apache.thrift.meta_data.FieldMetaData("fileID", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.CHUNK_ID, new org.apache.thrift.meta_data.FieldMetaData("chunkID", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ChunkIDs.class, metaDataMap);
  }

  public ChunkIDs() {
  }

  public ChunkIDs(
    String chunkID)
  {
    this();
    this.chunkID = chunkID;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ChunkIDs(ChunkIDs other) {
    if (other.isSetFileID()) {
      this.fileID = other.fileID;
    }
    if (other.isSetChunkID()) {
      this.chunkID = other.chunkID;
    }
  }

  public ChunkIDs deepCopy() {
    return new ChunkIDs(this);
  }

  @Override
  public void clear() {
    this.fileID = null;
    this.chunkID = null;
  }

  public String getFileID() {
    return this.fileID;
  }

  public ChunkIDs setFileID(String fileID) {
    this.fileID = fileID;
    return this;
  }

  public void unsetFileID() {
    this.fileID = null;
  }

  /** Returns true if field fileID is set (has been assigned a value) and false otherwise */
  public boolean isSetFileID() {
    return this.fileID != null;
  }

  public void setFileIDIsSet(boolean value) {
    if (!value) {
      this.fileID = null;
    }
  }

  public String getChunkID() {
    return this.chunkID;
  }

  public ChunkIDs setChunkID(String chunkID) {
    this.chunkID = chunkID;
    return this;
  }

  public void unsetChunkID() {
    this.chunkID = null;
  }

  /** Returns true if field chunkID is set (has been assigned a value) and false otherwise */
  public boolean isSetChunkID() {
    return this.chunkID != null;
  }

  public void setChunkIDIsSet(boolean value) {
    if (!value) {
      this.chunkID = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case FILE_ID:
      if (value == null) {
        unsetFileID();
      } else {
        setFileID((String)value);
      }
      break;

    case CHUNK_ID:
      if (value == null) {
        unsetChunkID();
      } else {
        setChunkID((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case FILE_ID:
      return getFileID();

    case CHUNK_ID:
      return getChunkID();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case FILE_ID:
      return isSetFileID();
    case CHUNK_ID:
      return isSetChunkID();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ChunkIDs)
      return this.equals((ChunkIDs)that);
    return false;
  }

  public boolean equals(ChunkIDs that) {
    if (that == null)
      return false;

    boolean this_present_fileID = true && this.isSetFileID();
    boolean that_present_fileID = true && that.isSetFileID();
    if (this_present_fileID || that_present_fileID) {
      if (!(this_present_fileID && that_present_fileID))
        return false;
      if (!this.fileID.equals(that.fileID))
        return false;
    }

    boolean this_present_chunkID = true && this.isSetChunkID();
    boolean that_present_chunkID = true && that.isSetChunkID();
    if (this_present_chunkID || that_present_chunkID) {
      if (!(this_present_chunkID && that_present_chunkID))
        return false;
      if (!this.chunkID.equals(that.chunkID))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(ChunkIDs other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    ChunkIDs typedOther = (ChunkIDs)other;

    lastComparison = Boolean.valueOf(isSetFileID()).compareTo(typedOther.isSetFileID());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetFileID()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.fileID, typedOther.fileID);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetChunkID()).compareTo(typedOther.isSetChunkID());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetChunkID()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.chunkID, typedOther.chunkID);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ChunkIDs(");
    boolean first = true;

    if (isSetFileID()) {
      sb.append("fileID:");
      if (this.fileID == null) {
        sb.append("null");
      } else {
        sb.append(this.fileID);
      }
      first = false;
    }
    if (!first) sb.append(", ");
    sb.append("chunkID:");
    if (this.chunkID == null) {
      sb.append("null");
    } else {
      sb.append(this.chunkID);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (chunkID == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'chunkID' was not present! Struct: " + toString());
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ChunkIDsStandardSchemeFactory implements SchemeFactory {
    public ChunkIDsStandardScheme getScheme() {
      return new ChunkIDsStandardScheme();
    }
  }

  private static class ChunkIDsStandardScheme extends StandardScheme<ChunkIDs> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ChunkIDs struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // FILE_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.fileID = iprot.readString();
              struct.setFileIDIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // CHUNK_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.chunkID = iprot.readString();
              struct.setChunkIDIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, ChunkIDs struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.fileID != null) {
        if (struct.isSetFileID()) {
          oprot.writeFieldBegin(FILE_ID_FIELD_DESC);
          oprot.writeString(struct.fileID);
          oprot.writeFieldEnd();
        }
      }
      if (struct.chunkID != null) {
        oprot.writeFieldBegin(CHUNK_ID_FIELD_DESC);
        oprot.writeString(struct.chunkID);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ChunkIDsTupleSchemeFactory implements SchemeFactory {
    public ChunkIDsTupleScheme getScheme() {
      return new ChunkIDsTupleScheme();
    }
  }

  private static class ChunkIDsTupleScheme extends TupleScheme<ChunkIDs> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ChunkIDs struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeString(struct.chunkID);
      BitSet optionals = new BitSet();
      if (struct.isSetFileID()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetFileID()) {
        oprot.writeString(struct.fileID);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ChunkIDs struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.chunkID = iprot.readString();
      struct.setChunkIDIsSet(true);
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        struct.fileID = iprot.readString();
        struct.setFileIDIsSet(true);
      }
    }
  }

}

