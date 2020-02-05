/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.tests.snapshot.jms;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class JmsMessageProducer {

    public static final String MESSAGE_PREFIX = "Message-";
    private static final int SLEEP_MILLIS = 50;

    private final Thread producerThread;
    private final String brokerURL;
    private final String queueName;

    private volatile boolean running = true;
    private volatile long totalCount;

    JmsMessageProducer(String brokerURL, String queueName) {
        producerThread = new Thread(() -> uncheckRun(this::run));
        this.brokerURL = brokerURL;
        this.queueName = queueName;
    }

    private void run() throws Exception {
        Connection connection = new ActiveMQConnectionFactory(brokerURL).createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(session.createQueue(queueName));

        long count = 0;
        while (running) {
            TextMessage textMessage = session.createTextMessage(MESSAGE_PREFIX + count++);
            producer.send(textMessage);
            MILLISECONDS.sleep(SLEEP_MILLIS);
        }
        producer.close();
        session.close();
        connection.close();
        totalCount = count;
    }

    void start() {
        producerThread.start();
    }

    long stop() throws InterruptedException {
        running = false;
        producerThread.join();
        return totalCount;
    }

}
