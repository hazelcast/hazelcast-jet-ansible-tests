/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.tests.remote;

import com.google.common.collect.Iterables;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.server.JetBootstrap;
import com.hazelcast.remotecontroller.Lang;
import com.hazelcast.remotecontroller.RemoteController;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.lang.Integer.parseInt;

/**
 * This class is used to connect to the HazelcastRemoteController
 * instances running on the cluster machines and restart
 * Hazelcast Jet instances one by one cycling through instances.
 *
 * A python script is invoked on the remote machine which
 * stops/starts a system service `hazelcast-jet-isolated`
 */
public final class RemoteControllerClient {

    private static final int DEFAULT_PORT = 9701;

    private RemoteControllerClient() {
    }

    public static void main(String[] args) throws Exception {
        String isolatedClientConfig = System.getProperty("isolatedClientConfig");
        if (isolatedClientConfig != null) {
            System.setProperty("hazelcast.client.config", isolatedClientConfig);
        }
        int initialSleep = parseInt(System.getProperty("initialSleepMinutes", "3"));
        int sleepAfterStop = parseInt(System.getProperty("sleepAfterStopMinutes", "1"));
        int sleepBetweenRestart = parseInt(System.getProperty("sleepBetweenRestartMinutes", "5"));
        long duration = TimeUnit.MINUTES.toMillis(parseInt(System.getProperty("durationMinutes", "30")));


        JetInstance jet = JetBootstrap.getInstance();
        HazelcastInstance instance = jet.getHazelcastInstance();

        Set<Member> members = instance.getCluster().getMembers();

        long begin = System.currentTimeMillis();
        sleepMinutes(initialSleep);
        Iterables.cycle(members).forEach(m -> {
            uncheckRun(() -> stop(m));
            uncheckRun(() -> sleepMinutes(sleepAfterStop));
            uncheckRun(() -> start(m));
            uncheckRun(() -> sleepMinutes(sleepBetweenRestart));
            if (System.currentTimeMillis() - begin > duration) {
                System.out.println("Exiting Remote Controller Client");
                System.exit(0);
            }
        });
    }

    private static void stop(Member member) throws Exception {
        call(member.getAddress().getHost(), "sudo initctl stop hazelcast-jet-isolated");
    }

    private static void start(Member member) throws Exception {
        call(member.getAddress().getHost(), "sudo initctl start hazelcast-jet-isolated");
    }

    private static void call(String host, String command) throws Exception {
        TTransport transport = new TSocket(host, DEFAULT_PORT);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        RemoteController.Client client = new RemoteController.Client(protocol);
        String script = "import subprocess\nprocess = subprocess.call(['" + command + "'], shell=True)";
        client.executeOnController(null, script, Lang.PYTHON);
        transport.close();
    }

    private static void sleepMinutes(int minutes) throws InterruptedException {
        TimeUnit.MINUTES.sleep(minutes);
    }
}
