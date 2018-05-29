/*
 * Copyright 2018, TeamDev. All rights reserved.
 *
 * Redistribution and use in source and/or binary forms, with or without
 * modification, must retain the above copyright notice and the following
 * disclaimer.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

"use strict";

let Any = require("spine-js-client-proto/google/protobuf/any_pb").Any;
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
