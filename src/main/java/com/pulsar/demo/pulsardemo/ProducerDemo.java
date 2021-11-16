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

/**
 * @author lixiaoshuang
 */
@Data
public class ProducerDemo {


    public static void main(String[] args) throws PulsarClientException {
        // 创建pulsar客户端
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://101.42.108.126:6650")
                .build();
        // 创建生产者
        Producer<byte[]> producer = client.newProducer().topic("test-topic-1").create();
        producer.send("发送一条字符串消息".getBytes());

        // 创建消费者
        Consumer<byte[]> consumer = client.newConsumer()
                .topic("test-topic-1")
                .subscriptionName("test-subscription-1")
                .subscribe();

        //获取消息内容
        Message<byte[]> message = consumer.receive();
        System.out.println("接受消息内容: " + new String(message.getData()));
        // 确认消费成功，以便pulsar删除消费成功的消息
        consumer.acknowledge(message);

        //关闭客户端
        producer.close();
        consumer.close();
        client.close();
    }
}
