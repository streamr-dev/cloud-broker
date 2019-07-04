package com.streamr.broker;

import com.streamr.broker.stats.Stats;
import com.streamr.client.protocol.control_layer.PublishRequest;
import com.streamr.client.protocol.message_layer.StreamMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.NotYetConnectedException;
import java.util.ArrayList;

public class TestNetReporter implements Reporter {
    private static final Logger log = LogManager.getLogger();
    private Stats stats;
    private ArrayList<WebSocketClient> clients = new ArrayList<>();

    public TestNetReporter(String[] nodeAdresses) {
        for (String address: nodeAdresses) {
            clients.add(getWebSocketClient(address));
        }
    }

    @Override
    public void setStats(Stats stats) {
        this.stats = stats;
    }

    @Override
    public void report(StreamMessage msg) {
        String key = msg.getStreamId() + msg.getStreamPartition();
        WebSocketClient client = clients.get(key.hashCode() % clients.size());
        PublishRequest request = new PublishRequest(msg, null);
        client.send(request.toJson());
    }

    @Override
    public void close() {

    }

    private WebSocketClient getWebSocketClient(String address) {
        try {
            return new WebSocketClient(new URI(address)) {
                @Override
                public void onOpen(ServerHandshake handshakedata) {
                    log.info("Connection established");
                }

                @Override
                public void onMessage(String message) {

                }

                @Override
                public void onClose(int code, String reason, boolean remote) {
                    log.info("Connection closed! Code: " + code + ", Reason: " + reason);
                }

                @Override
                public void onError(Exception ex) {
                    log.error(ex);
                }

                @Override
                public void send(String text) throws NotYetConnectedException {
                    log.info(">> " + text);
                    super.send(text);
                }
            };
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
