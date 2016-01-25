/*
 * Copyright 2014 Mariam Hakobyan
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
package org.elasticsearch.river.kafka;

import kafka.message.MessageAndMetadata;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.ChannelBufferBytesReference;
import org.elasticsearch.common.lang3.StringUtils;
import org.elasticsearch.common.netty.buffer.ByteBufferBackedChannelBuffer;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Producer to index documents. Creates index document requests, which are executed with Bulk API.
 */
public class DefaultDocumentProducer extends ElasticSearchProducer {


    public DefaultDocumentProducer(Client client, RiverConfig riverConfig, KafkaConsumer kafkaConsumer, Stats stats) {
        super(client, riverConfig, kafkaConsumer, stats);
    }

    /**
     * For the given messages creates index document requests and adds them to the bulk processor queue, for
     * processing later when the size of bulk actions is reached.
     *
     * @param messageAndMetadata given message
     */
    public void addMessagesToBulkProcessor(final MessageAndMetadata messageAndMetadata) {

        final byte[] messageBytes = (byte[]) messageAndMetadata.message();

        if (messageBytes == null || messageBytes.length == 0) return;

        try {

            switch (riverConfig.getMessageType()) {
                case STRING:
                    String message = XContentFactory.jsonBuilder()
                            .startObject()
                            .field("value", new String(messageBytes, "UTF-8"))
                            .endObject()
                            .string();
                    IndexRequest request = Requests.indexRequest(riverConfig.getIndexName()).
                            type(riverConfig.getTypeName()).
                            source(message);
                    bulkProcessor.add(request);
                    break;
                case JSON:
                    Map<String, Object> messageMap = reader.readValue(messageBytes);
                    String optype = null;
                    if (messageMap.containsKey("optype")) {
                        optype = messageMap.get("optype").toString();
                    }
                    String indexName = riverConfig.getIndexName();
                    if (messageMap.containsKey("esIndex") && StringUtils.isNotEmpty(messageMap.get("esIndex").toString())) {
                        indexName = messageMap.get("esIndex").toString();
                    } else {
                        if (riverConfig.isDynamicIndex()) {
                            if (messageMap.get("fields") instanceof Map) {
                                indexName = indexName.replaceFirst("\\$\\{(.+)\\}", ((Map) messageMap.get("fields")).get(riverConfig.getDynamicIndexField()).toString());
                            } else {
                                throw new IllegalArgumentException(" message fields is not a map type");
                            }
                        }
                    }
                    String typeName = riverConfig.getTypeName();
                    if (messageMap.containsKey("esType") && StringUtils.isNotEmpty(messageMap.get("esType").toString())) {
                        typeName = messageMap.get("esType").toString();
                    } else {
                        if (riverConfig.isDynamicType()) {
                            if (messageMap.get("fields") instanceof Map) {
                                typeName = typeName.replaceFirst("\\$\\{(.+)\\}", ((Map) messageMap.get("fields")).get(riverConfig.getDynamicTypeField()).toString());
                            } else {
                                throw new IllegalArgumentException(" message fields is not a map type");
                            }
                        }
                    }

                    if ("index".equals(optype)) {
                        addDoc(indexName, typeName, messageMap);
                    } else if ("update".equals(optype)) {
                        updateDoc(indexName, typeName, messageMap);
                    } else if ("delete".equals(optype)) {
                        deleteDoc(indexName, typeName, messageMap);
                    } else {
                        throw new IllegalArgumentException("unknown optype! " + "raw message: " + objectMapper.writeValueAsString(messageMap));
                    }
            }

        } catch (Exception ex) {
            logger.error("=============process message error:" + ex.getMessage());
            ex.printStackTrace();
        }
    }

    private void addDoc(String indexName, String typeName, Map<String, Object> messageMap) {
//        logger.info("========================indexName:" + indexName + ",  messageMap:" + messageMap + ",  fields:" + messageMap.get("fields"));
        String id = null;
        if (messageMap.containsKey("id")) {
            id = messageMap.get("id").toString();
        }
        if (messageMap.get("fields") instanceof Map) {
            IndexRequest request = Requests.indexRequest(indexName).
                    type(typeName).
                    source((Map) messageMap.get("fields"));
            if (id != null) {
                request.id(id);
            }
            bulkProcessor.add(request);
        } else {
            throw new IllegalArgumentException(" message fields is not a map type");
        }
    }

    private void updateDoc(String indexName, String typeName, Map<String, Object> messageMap) {
        if (messageMap.containsKey("id")) {
            String id = messageMap.get("id").toString();
            if (messageMap.get("fields") instanceof Map) {
                final UpdateRequest request = new UpdateRequest(indexName, typeName, id).doc((Map) messageMap.get("fields"));
                bulkProcessor.add(request);
            } else {
                throw new IllegalArgumentException(" message fields is not a map type");
            }
        } else {
            throw new IllegalArgumentException("No id provided in a message to update a document");
        }
    }

    private void deleteDoc(String indexName, String typeName, Map<String, Object> messageMap) {
        if (messageMap.containsKey("id")) {
            String id = messageMap.get("id").toString();

            final DeleteRequest request = Requests.deleteRequest(indexName).
                    type(typeName).
                    id(id);

            bulkProcessor.add(request);
        } else {
            throw new IllegalArgumentException("No id provided in a message to delete a document");
        }
    }

//    private void rawExecute(String indexName, String typeName, byte[] messageBytes) {
//        try {
//            ByteBuffer byteBuffer = ByteBuffer.wrap(messageBytes);
//            bulkProcessor.add(
//                    new ChannelBufferBytesReference(new ByteBufferBackedChannelBuffer(byteBuffer)),
//                    indexName,
//                    typeName
//            );
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
}
