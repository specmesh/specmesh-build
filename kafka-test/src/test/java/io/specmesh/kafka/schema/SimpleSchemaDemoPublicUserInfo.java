/*
 * Copyright 2023 SpecMesh Contributors (https://github.com/specmesh)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: simple.schema_demo._public.user_info.proto

package io.specmesh.kafka.schema;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressWarnings("all")
@SuppressFBWarnings
public final class SimpleSchemaDemoPublicUserInfo {
    private SimpleSchemaDemoPublicUserInfo() {}

    public static void registerAllExtensions(com.google.protobuf.ExtensionRegistryLite registry) {}

    public static void registerAllExtensions(com.google.protobuf.ExtensionRegistry registry) {
        registerAllExtensions((com.google.protobuf.ExtensionRegistryLite) registry);
    }

    public interface UserInfoOrBuilder
            extends
            // @@protoc_insertion_point(interface_extends:io.specmesh.kafka.schema.UserInfo)
            com.google.protobuf.MessageOrBuilder {

        /**
         * <code>string fullName = 1;</code>
         *
         * @return The fullName.
         */
        java.lang.String getFullName();
        /**
         * <code>string fullName = 1;</code>
         *
         * @return The bytes for fullName.
         */
        com.google.protobuf.ByteString getFullNameBytes();

        /**
         * <code>string email = 2;</code>
         *
         * @return The email.
         */
        java.lang.String getEmail();
        /**
         * <code>string email = 2;</code>
         *
         * @return The bytes for email.
         */
        com.google.protobuf.ByteString getEmailBytes();

        /**
         * <code>int32 age = 3;</code>
         *
         * @return The age.
         */
        int getAge();
    }
    /** Protobuf type {@code io.specmesh.kafka.schema.UserInfo} */
    public static final class UserInfo extends com.google.protobuf.GeneratedMessageV3
            implements
            // @@protoc_insertion_point(message_implements:io.specmesh.kafka.schema.UserInfo)
            UserInfoOrBuilder {
        private static final long serialVersionUID = 0L;
        // Use UserInfo.newBuilder() to construct.
        private UserInfo(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
            super(builder);
        }

        private UserInfo() {
            fullName_ = "";
            email_ = "";
        }

        @java.lang.Override
        @SuppressWarnings({"unused"})
        protected java.lang.Object newInstance(UnusedPrivateParameter unused) {
            return new UserInfo();
        }

        @java.lang.Override
        public final com.google.protobuf.UnknownFieldSet getUnknownFields() {
            return this.unknownFields;
        }

        public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
            return io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo
                    .internal_static_io_specmesh_kafka_schema_UserInfo_descriptor;
        }

        @java.lang.Override
        protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
                internalGetFieldAccessorTable() {
            return io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo
                    .internal_static_io_specmesh_kafka_schema_UserInfo_fieldAccessorTable
                    .ensureFieldAccessorsInitialized(
                            io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo.class,
                            io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo.Builder
                                    .class);
        }

        public static final int FULLNAME_FIELD_NUMBER = 1;

        @SuppressWarnings("serial")
        private volatile java.lang.Object fullName_ = "";
        /**
         * <code>string fullName = 1;</code>
         *
         * @return The fullName.
         */
        @java.lang.Override
        public java.lang.String getFullName() {
            java.lang.Object ref = fullName_;
            if (ref instanceof java.lang.String) {
                return (java.lang.String) ref;
            } else {
                com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) ref;
                java.lang.String s = bs.toStringUtf8();
                fullName_ = s;
                return s;
            }
        }
        /**
         * <code>string fullName = 1;</code>
         *
         * @return The bytes for fullName.
         */
        @java.lang.Override
        public com.google.protobuf.ByteString getFullNameBytes() {
            java.lang.Object ref = fullName_;
            if (ref instanceof java.lang.String) {
                com.google.protobuf.ByteString b =
                        com.google.protobuf.ByteString.copyFromUtf8((java.lang.String) ref);
                fullName_ = b;
                return b;
            } else {
                return (com.google.protobuf.ByteString) ref;
            }
        }

        public static final int EMAIL_FIELD_NUMBER = 2;

        @SuppressWarnings("serial")
        private volatile java.lang.Object email_ = "";
        /**
         * <code>string email = 2;</code>
         *
         * @return The email.
         */
        @java.lang.Override
        public java.lang.String getEmail() {
            java.lang.Object ref = email_;
            if (ref instanceof java.lang.String) {
                return (java.lang.String) ref;
            } else {
                com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) ref;
                java.lang.String s = bs.toStringUtf8();
                email_ = s;
                return s;
            }
        }
        /**
         * <code>string email = 2;</code>
         *
         * @return The bytes for email.
         */
        @java.lang.Override
        public com.google.protobuf.ByteString getEmailBytes() {
            java.lang.Object ref = email_;
            if (ref instanceof java.lang.String) {
                com.google.protobuf.ByteString b =
                        com.google.protobuf.ByteString.copyFromUtf8((java.lang.String) ref);
                email_ = b;
                return b;
            } else {
                return (com.google.protobuf.ByteString) ref;
            }
        }

        public static final int AGE_FIELD_NUMBER = 3;
        private int age_ = 0;
        /**
         * <code>int32 age = 3;</code>
         *
         * @return The age.
         */
        @java.lang.Override
        public int getAge() {
            return age_;
        }

        private byte memoizedIsInitialized = -1;

        @java.lang.Override
        public final boolean isInitialized() {
            byte isInitialized = memoizedIsInitialized;
            if (isInitialized == 1) return true;
            if (isInitialized == 0) return false;

            memoizedIsInitialized = 1;
            return true;
        }

        @java.lang.Override
        public void writeTo(com.google.protobuf.CodedOutputStream output)
                throws java.io.IOException {
            if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(fullName_)) {
                com.google.protobuf.GeneratedMessageV3.writeString(output, 1, fullName_);
            }
            if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(email_)) {
                com.google.protobuf.GeneratedMessageV3.writeString(output, 2, email_);
            }
            if (age_ != 0) {
                output.writeInt32(3, age_);
            }
            getUnknownFields().writeTo(output);
        }

        @java.lang.Override
        public int getSerializedSize() {
            int size = memoizedSize;
            if (size != -1) return size;

            size = 0;
            if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(fullName_)) {
                size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, fullName_);
            }
            if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(email_)) {
                size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, email_);
            }
            if (age_ != 0) {
                size += com.google.protobuf.CodedOutputStream.computeInt32Size(3, age_);
            }
            size += getUnknownFields().getSerializedSize();
            memoizedSize = size;
            return size;
        }

        @java.lang.Override
        public boolean equals(final java.lang.Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj
                    instanceof io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo)) {
                return super.equals(obj);
            }
            io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo other =
                    (io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo) obj;

            if (!getFullName().equals(other.getFullName())) return false;
            if (!getEmail().equals(other.getEmail())) return false;
            if (getAge() != other.getAge()) return false;
            if (!getUnknownFields().equals(other.getUnknownFields())) return false;
            return true;
        }

        @java.lang.Override
        public int hashCode() {
            if (memoizedHashCode != 0) {
                return memoizedHashCode;
            }
            int hash = 41;
            hash = (19 * hash) + getDescriptor().hashCode();
            hash = (37 * hash) + FULLNAME_FIELD_NUMBER;
            hash = (53 * hash) + getFullName().hashCode();
            hash = (37 * hash) + EMAIL_FIELD_NUMBER;
            hash = (53 * hash) + getEmail().hashCode();
            hash = (37 * hash) + AGE_FIELD_NUMBER;
            hash = (53 * hash) + getAge();
            hash = (29 * hash) + getUnknownFields().hashCode();
            memoizedHashCode = hash;
            return hash;
        }

        public static io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo parseFrom(
                java.nio.ByteBuffer data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo parseFrom(
                java.nio.ByteBuffer data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo parseFrom(
                com.google.protobuf.ByteString data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo parseFrom(
                com.google.protobuf.ByteString data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo parseFrom(
                byte[] data) throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo parseFrom(
                byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo parseFrom(
                java.io.InputStream input) throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo parseFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo
                parseDelimitedFrom(java.io.InputStream input) throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input);
        }

        public static io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo
                parseDelimitedFrom(
                        java.io.InputStream input,
                        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                        throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
                    PARSER, input, extensionRegistry);
        }

        public static io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo parseFrom(
                com.google.protobuf.CodedInputStream input) throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
        }

        public static io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo parseFrom(
                com.google.protobuf.CodedInputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
                    PARSER, input, extensionRegistry);
        }

        @java.lang.Override
        public Builder newBuilderForType() {
            return newBuilder();
        }

        public static Builder newBuilder() {
            return DEFAULT_INSTANCE.toBuilder();
        }

        public static Builder newBuilder(
                io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo prototype) {
            return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
        }

        @java.lang.Override
        public Builder toBuilder() {
            return this == DEFAULT_INSTANCE ? new Builder() : new Builder().mergeFrom(this);
        }

        @java.lang.Override
        protected Builder newBuilderForType(
                com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
            Builder builder = new Builder(parent);
            return builder;
        }
        /** Protobuf type {@code io.specmesh.kafka.schema.UserInfo} */
        public static final class Builder
                extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
                implements
                // @@protoc_insertion_point(builder_implements:io.specmesh.kafka.schema.UserInfo)
                io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfoOrBuilder {
            public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
                return io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo
                        .internal_static_io_specmesh_kafka_schema_UserInfo_descriptor;
            }

            @java.lang.Override
            protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
                    internalGetFieldAccessorTable() {
                return io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo
                        .internal_static_io_specmesh_kafka_schema_UserInfo_fieldAccessorTable
                        .ensureFieldAccessorsInitialized(
                                io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo
                                        .class,
                                io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo
                                        .Builder.class);
            }

            // Construct using
            // io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo.newBuilder()
            private Builder() {}

            private Builder(com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
                super(parent);
            }

            @java.lang.Override
            public Builder clear() {
                super.clear();
                bitField0_ = 0;
                fullName_ = "";
                email_ = "";
                age_ = 0;
                return this;
            }

            @java.lang.Override
            public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
                return io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo
                        .internal_static_io_specmesh_kafka_schema_UserInfo_descriptor;
            }

            @java.lang.Override
            public io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo
                    getDefaultInstanceForType() {
                return io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo
                        .getDefaultInstance();
            }

            @java.lang.Override
            public io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo build() {
                io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo result =
                        buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result);
                }
                return result;
            }

            @java.lang.Override
            public io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo buildPartial() {
                io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo result =
                        new io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo(this);
                if (bitField0_ != 0) {
                    buildPartial0(result);
                }
                onBuilt();
                return result;
            }

            private void buildPartial0(
                    io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo result) {
                int from_bitField0_ = bitField0_;
                if (((from_bitField0_ & 0x00000001) != 0)) {
                    result.fullName_ = fullName_;
                }
                if (((from_bitField0_ & 0x00000002) != 0)) {
                    result.email_ = email_;
                }
                if (((from_bitField0_ & 0x00000004) != 0)) {
                    result.age_ = age_;
                }
            }

            @java.lang.Override
            public Builder clone() {
                return super.clone();
            }

            @java.lang.Override
            public Builder setField(
                    com.google.protobuf.Descriptors.FieldDescriptor field, java.lang.Object value) {
                return super.setField(field, value);
            }

            @java.lang.Override
            public Builder clearField(com.google.protobuf.Descriptors.FieldDescriptor field) {
                return super.clearField(field);
            }

            @java.lang.Override
            public Builder clearOneof(com.google.protobuf.Descriptors.OneofDescriptor oneof) {
                return super.clearOneof(oneof);
            }

            @java.lang.Override
            public Builder setRepeatedField(
                    com.google.protobuf.Descriptors.FieldDescriptor field,
                    int index,
                    java.lang.Object value) {
                return super.setRepeatedField(field, index, value);
            }

            @java.lang.Override
            public Builder addRepeatedField(
                    com.google.protobuf.Descriptors.FieldDescriptor field, java.lang.Object value) {
                return super.addRepeatedField(field, value);
            }

            @java.lang.Override
            public Builder mergeFrom(com.google.protobuf.Message other) {
                if (other
                        instanceof
                        io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo) {
                    return mergeFrom(
                            (io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo)
                                    other);
                } else {
                    super.mergeFrom(other);
                    return this;
                }
            }

            public Builder mergeFrom(
                    io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo other) {
                if (other
                        == io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo
                                .getDefaultInstance()) return this;
                if (!other.getFullName().isEmpty()) {
                    fullName_ = other.fullName_;
                    bitField0_ |= 0x00000001;
                    onChanged();
                }
                if (!other.getEmail().isEmpty()) {
                    email_ = other.email_;
                    bitField0_ |= 0x00000002;
                    onChanged();
                }
                if (other.getAge() != 0) {
                    setAge(other.getAge());
                }
                this.mergeUnknownFields(other.getUnknownFields());
                onChanged();
                return this;
            }

            @java.lang.Override
            public final boolean isInitialized() {
                return true;
            }

            @java.lang.Override
            public Builder mergeFrom(
                    com.google.protobuf.CodedInputStream input,
                    com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                    throws java.io.IOException {
                if (extensionRegistry == null) {
                    throw new java.lang.NullPointerException();
                }
                try {
                    boolean done = false;
                    while (!done) {
                        int tag = input.readTag();
                        switch (tag) {
                            case 0:
                                done = true;
                                break;
                            case 10:
                                {
                                    fullName_ = input.readStringRequireUtf8();
                                    bitField0_ |= 0x00000001;
                                    break;
                                } // case 10
                            case 18:
                                {
                                    email_ = input.readStringRequireUtf8();
                                    bitField0_ |= 0x00000002;
                                    break;
                                } // case 18
                            case 24:
                                {
                                    age_ = input.readInt32();
                                    bitField0_ |= 0x00000004;
                                    break;
                                } // case 24
                            default:
                                {
                                    if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                                        done = true; // was an endgroup tag
                                    }
                                    break;
                                } // default:
                        } // switch (tag)
                    } // while (!done)
                } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                    throw e.unwrapIOException();
                } finally {
                    onChanged();
                } // finally
                return this;
            }

            private int bitField0_;

            private java.lang.Object fullName_ = "";
            /**
             * <code>string fullName = 1;</code>
             *
             * @return The fullName.
             */
            public java.lang.String getFullName() {
                java.lang.Object ref = fullName_;
                if (!(ref instanceof java.lang.String)) {
                    com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) ref;
                    java.lang.String s = bs.toStringUtf8();
                    fullName_ = s;
                    return s;
                } else {
                    return (java.lang.String) ref;
                }
            }
            /**
             * <code>string fullName = 1;</code>
             *
             * @return The bytes for fullName.
             */
            public com.google.protobuf.ByteString getFullNameBytes() {
                java.lang.Object ref = fullName_;
                if (ref instanceof String) {
                    com.google.protobuf.ByteString b =
                            com.google.protobuf.ByteString.copyFromUtf8((java.lang.String) ref);
                    fullName_ = b;
                    return b;
                } else {
                    return (com.google.protobuf.ByteString) ref;
                }
            }
            /**
             * <code>string fullName = 1;</code>
             *
             * @param value The fullName to set.
             * @return This builder for chaining.
             */
            public Builder setFullName(java.lang.String value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                fullName_ = value;
                bitField0_ |= 0x00000001;
                onChanged();
                return this;
            }
            /**
             * <code>string fullName = 1;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearFullName() {
                fullName_ = getDefaultInstance().getFullName();
                bitField0_ = (bitField0_ & ~0x00000001);
                onChanged();
                return this;
            }
            /**
             * <code>string fullName = 1;</code>
             *
             * @param value The bytes for fullName to set.
             * @return This builder for chaining.
             */
            public Builder setFullNameBytes(com.google.protobuf.ByteString value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                checkByteStringIsUtf8(value);
                fullName_ = value;
                bitField0_ |= 0x00000001;
                onChanged();
                return this;
            }

            private java.lang.Object email_ = "";
            /**
             * <code>string email = 2;</code>
             *
             * @return The email.
             */
            public java.lang.String getEmail() {
                java.lang.Object ref = email_;
                if (!(ref instanceof java.lang.String)) {
                    com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) ref;
                    java.lang.String s = bs.toStringUtf8();
                    email_ = s;
                    return s;
                } else {
                    return (java.lang.String) ref;
                }
            }
            /**
             * <code>string email = 2;</code>
             *
             * @return The bytes for email.
             */
            public com.google.protobuf.ByteString getEmailBytes() {
                java.lang.Object ref = email_;
                if (ref instanceof String) {
                    com.google.protobuf.ByteString b =
                            com.google.protobuf.ByteString.copyFromUtf8((java.lang.String) ref);
                    email_ = b;
                    return b;
                } else {
                    return (com.google.protobuf.ByteString) ref;
                }
            }
            /**
             * <code>string email = 2;</code>
             *
             * @param value The email to set.
             * @return This builder for chaining.
             */
            public Builder setEmail(java.lang.String value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                email_ = value;
                bitField0_ |= 0x00000002;
                onChanged();
                return this;
            }
            /**
             * <code>string email = 2;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearEmail() {
                email_ = getDefaultInstance().getEmail();
                bitField0_ = (bitField0_ & ~0x00000002);
                onChanged();
                return this;
            }
            /**
             * <code>string email = 2;</code>
             *
             * @param value The bytes for email to set.
             * @return This builder for chaining.
             */
            public Builder setEmailBytes(com.google.protobuf.ByteString value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                checkByteStringIsUtf8(value);
                email_ = value;
                bitField0_ |= 0x00000002;
                onChanged();
                return this;
            }

            private int age_;
            /**
             * <code>int32 age = 3;</code>
             *
             * @return The age.
             */
            @java.lang.Override
            public int getAge() {
                return age_;
            }
            /**
             * <code>int32 age = 3;</code>
             *
             * @param value The age to set.
             * @return This builder for chaining.
             */
            public Builder setAge(int value) {

                age_ = value;
                bitField0_ |= 0x00000004;
                onChanged();
                return this;
            }
            /**
             * <code>int32 age = 3;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearAge() {
                bitField0_ = (bitField0_ & ~0x00000004);
                age_ = 0;
                onChanged();
                return this;
            }

            @java.lang.Override
            public final Builder setUnknownFields(
                    final com.google.protobuf.UnknownFieldSet unknownFields) {
                return super.setUnknownFields(unknownFields);
            }

            @java.lang.Override
            public final Builder mergeUnknownFields(
                    final com.google.protobuf.UnknownFieldSet unknownFields) {
                return super.mergeUnknownFields(unknownFields);
            }

            // @@protoc_insertion_point(builder_scope:io.specmesh.kafka.schema.UserInfo)
        }

        // @@protoc_insertion_point(class_scope:io.specmesh.kafka.schema.UserInfo)
        private static final io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo
                DEFAULT_INSTANCE;

        static {
            DEFAULT_INSTANCE =
                    new io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo();
        }

        public static io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo
                getDefaultInstance() {
            return DEFAULT_INSTANCE;
        }

        private static final com.google.protobuf.Parser<UserInfo> PARSER =
                new com.google.protobuf.AbstractParser<UserInfo>() {
                    @java.lang.Override
                    public UserInfo parsePartialFrom(
                            com.google.protobuf.CodedInputStream input,
                            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                            throws com.google.protobuf.InvalidProtocolBufferException {
                        Builder builder = newBuilder();
                        try {
                            builder.mergeFrom(input, extensionRegistry);
                        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                            throw e.setUnfinishedMessage(builder.buildPartial());
                        } catch (com.google.protobuf.UninitializedMessageException e) {
                            throw e.asInvalidProtocolBufferException()
                                    .setUnfinishedMessage(builder.buildPartial());
                        } catch (java.io.IOException e) {
                            throw new com.google.protobuf.InvalidProtocolBufferException(e)
                                    .setUnfinishedMessage(builder.buildPartial());
                        }
                        return builder.buildPartial();
                    }
                };

        public static com.google.protobuf.Parser<UserInfo> parser() {
            return PARSER;
        }

        @java.lang.Override
        public com.google.protobuf.Parser<UserInfo> getParserForType() {
            return PARSER;
        }

        @java.lang.Override
        public io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo
                getDefaultInstanceForType() {
            return DEFAULT_INSTANCE;
        }
    }

    private static final com.google.protobuf.Descriptors.Descriptor
            internal_static_io_specmesh_kafka_schema_UserInfo_descriptor;
    private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
            internal_static_io_specmesh_kafka_schema_UserInfo_fieldAccessorTable;

    public static com.google.protobuf.Descriptors.FileDescriptor getDescriptor() {
        return descriptor;
    }

    private static com.google.protobuf.Descriptors.FileDescriptor descriptor;

    static {
        java.lang.String[] descriptorData = {
            "\n"
                + "*simple.schema_demo._public.user_info.proto\022\030io.specmesh.kafka.schema\"8\n"
                + "\010UserInfo\022\020\n"
                + "\010fullName\030\001 \001(\t\022\r\n"
                + "\005email\030\002 \001(\t\022\013\n"
                + "\003age\030\003 \001(\005b\006proto3"
        };
        descriptor =
                com.google.protobuf.Descriptors.FileDescriptor.internalBuildGeneratedFileFrom(
                        descriptorData, new com.google.protobuf.Descriptors.FileDescriptor[] {});
        internal_static_io_specmesh_kafka_schema_UserInfo_descriptor =
                getDescriptor().getMessageTypes().get(0);
        internal_static_io_specmesh_kafka_schema_UserInfo_fieldAccessorTable =
                new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
                        internal_static_io_specmesh_kafka_schema_UserInfo_descriptor,
                        new java.lang.String[] {
                            "FullName", "Email", "Age",
                        });
    }

    // @@protoc_insertion_point(outer_class_scope)
}
