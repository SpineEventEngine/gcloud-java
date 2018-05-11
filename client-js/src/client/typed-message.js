/*
 * Copyright (c) 2000-2018 TeamDev. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

"use strict";

let Any = require("../../proto/main/js/google/protobuf/any_pb").Any;
let base64 = require("base64-js");

/**
 * A URL of a Protobuf type.
 *
 * Consists of the two parts separated with a slash. The first part is
 * the type URL prefix (for example, {@code "type.googleapis.com"}).
 * The second part is a fully-qualified Protobuf type name.
 */
export class TypeUrl {

  /**
   * Creates a new instance of TypeUrl from the given string value.
   *
   * The value should be a valid type URL of format:
   * (typeUrlPrefix)/(typeName)
   *
   * @param value the type URL value
   */
  constructor(value) {
    let urlParts = value.split("/");
    this.typeUrlPrefix = urlParts[0];
    this.typeName = urlParts[1];
    this.value = value;
  }
}

/**
 * A Protobuf message with a {@link TypeUrl}.
 *
 * The type URL specifies the type of the associated message.
 */
export class TypedMessage {

  /**
   * Creates a new instance of TypedMessage from the given Protobuf message and
   * type URL.
   *
   * @param message the Protobuf message
   * @param typeUrl the {@link TypeUrl} representing the type of the message
   */
  constructor(message, typeUrl) {
    this.message = message;
    this.type = typeUrl;
  }

  /**
   * Converts this message into a byte array.
   *
   * @returns an array of bytes representing the message
   */
  toBytes() {
    let result = this.message.serializeBinary();
    return result;
  }

  /**
   * Converts this message into an {@link Any}.
   *
   * @returns this message packed into an instance of Any
   */
  toAny() {
    let result = new Any();
    let bytes = this.toBytes();
    result.pack(bytes, this.type.typeName, this.type.typeUrlPrefix);
    return result;
  }

  /**
   * Converts this message into a Base64-encoded byte string.
   *
   * @returns the string representing this message
   */
  toBase64() {
    let bytes = this.toBytes();
    let result = base64.fromByteArray(bytes);
    return result;
  }
}
