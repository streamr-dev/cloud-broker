package com.streamr.broker;

import com.streamr.broker.stats.Stats;
import com.streamr.client.protocol.control_layer.PublishRequest;
import com.streamr.client.protocol.message_layer.StreamMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.exceptions.WebsocketNotConnectedException;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.NotYetConnectedException;
import java.util.ArrayList;
import java.util.Arrays;

public class TestNetReporter implements Reporter {
    private static final Logger log = LogManager.getLogger();
    private Stats stats;
    private ArrayList<WebSocketClient> clients = new ArrayList<>();

    public TestNetReporter(String[] nodeAddresses) {
        log.info("Connecting to nodes: " + Arrays.toString(nodeAddresses));

        for (String address: nodeAddresses) {
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
        WebSocketClient client = clients.get(Math.abs(key.hashCode()) % clients.size());
        PublishRequest request = new PublishRequest(msg, null);

        // Try with the client computed by the hash
        if (!trySend(request, client)) {
            // If unsuccessful, try the other clients in order
            int i = 0;
            for (; i<clients.size(); i++) {
                if (clients.get(i) != client) {
                    if (trySend(request, clients.get(i))) {
                        break;
                    }
                }
            }
            if (i == clients.size()) {
                log.error("Failed to send message to any node!");
            }
        }
        stats.onWrittenToCassandra(msg);
    }

    private boolean trySend(PublishRequest request, WebSocketClient client) {
        try {
            client.send(request.toJson());
        } catch (WebsocketNotConnectedException e) {
            log.error("Client is not connected! Trying to reconnect: " + client.getURI());
            // Try to reconnect the websocket
            try {
                client.reconnectBlocking();
                log.info("Reconnected to " + client.getURI() + ". Trying again to send the message.");
                client.send(request.toJson());
            } catch (Exception ee) {
                log.info("Unable to reconnect or resend message to " + client.getURI() + ". Trying another node.");
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() {

    }

    private WebSocketClient getWebSocketClient(String address) {
        try {
            WebSocketClient ws = new WebSocketClient(new URI(address)) {
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
                    super.send(text);
                }
            };
            log.info("Connecting to: " + address);
            ws.connectBlocking();
            log.info("Connected to: " + address);
            return ws;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
