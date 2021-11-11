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
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://101.42.108.126:6650")
                .build();
        Producer<byte[]> producer = client.newProducer().topic("lxs").create();

        producer.send("发送消息 哈哈哈".getBytes());


        Consumer<byte[]> consumer = client.newConsumer()
                .topic("lxs")
                .subscriptionName("my-subscription")
                .subscribe();

        Message<byte[]> receive = consumer.receive();
        System.out.println(receive.getTopicName());

    }
}
