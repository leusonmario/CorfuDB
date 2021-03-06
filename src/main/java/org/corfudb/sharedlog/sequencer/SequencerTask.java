/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.corfudb.sharedlog.sequencer;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import org.corfudb.sharedlog.ICorfuDBServer;
import java.util.Map;

public class SequencerTask implements SequencerService.Iface, ICorfuDBServer {

    public int port = 0;

	AtomicLong pos = new AtomicLong(0);

	@Override
	public long nextpos(int range) throws org.apache.thrift.TException {
		// if (pos % 10000 == 0) System.out.println("issue token " + pos + "...");
		long ret = pos.getAndAdd(range);
		return ret;
	}

    @Override
    public void recover(long lowbound) throws TException {
        pos.set(lowbound);
    }

    public Runnable getInstance(final Map<String,Object> config)
    {
        final SequencerTask st = this;
        return new Runnable()
        {
            @Override
            public void run() {
                st.port = (Integer) config.get("port");
                st.serverloop();
            }
        };
    }

	public void serverloop() {

        TServer server;
        TServerSocket serverTransport;
        SequencerService.Processor<SequencerTask> processor;
        System.out.println("run..");

        try {
            serverTransport = new TServerSocket(port);
            processor =
                    new SequencerService.Processor<SequencerTask>(this);
            server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
            System.out.println("Starting sequencer on port " + port);

            server.serve();
        } catch (TTransportException e) {
            e.printStackTrace();
        }
	}
}
