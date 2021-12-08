/*
 * Copyright 2021 Gypsophila open source organization.
 *
 * Licensed under the Apache License,Version2.0(the"License");
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

package com.pulsar.demo.pulsardemo;

import lombok.Data;
import org.apache.pulsar.client.api.*;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author lixiaoshuang
 */
@Data
public class ProducerDemo {
    
    private static PulsarClient PULSAR_CLIENT = null;
    
    static {
        try {
            // 创建pulsar客户端
            PULSAR_CLIENT = PulsarClient.builder().serviceUrl("pulsar://101.42.108.126:6650").build();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }
    
    
    public static void main(String[] args) throws PulsarClientException, ExecutionException, InterruptedException {
        
        // 发送方式测试
        sendTest();
        
        // 创建消费者
        Consumer<byte[]> consumer = PULSAR_CLIENT.newConsumer().topic("test-topic-1")
                .subscriptionName("test-subscription-1").subscribe();
        
        //获取消息内容
        Message<byte[]> message = consumer.receive();
        System.out.println("接受消息内容: " + new String(message.getData()));
        // 确认消费成功，以便pulsar删除消费成功的消息
        consumer.acknowledge(message);
        
        //关闭客户端
        //        producer.close();
        consumer.close();
        PULSAR_CLIENT.close();
    }
    
    private static void sendTest() throws PulsarClientException, InterruptedException, ExecutionException {
        // 创建生产者
        Producer<byte[]> producer = PULSAR_CLIENT.newProducer().topic("test-topic-1").create();
        // 同步发送消息
        MessageId messageId = producer.send("同步发送的消息".getBytes(StandardCharsets.UTF_8));
        System.out.println("同步发送，消息id: " + messageId);
        
        CompletableFuture<MessageId> messageIdCompletableFuture = producer.sendAsync(
                "异步发送的消息".getBytes(StandardCharsets.UTF_8));
        System.out.println("异步发送，消息id：" + messageIdCompletableFuture.get());
    }
}
