package com.streamr.broker.stats;

import com.streamr.client.StreamrClient;
import com.streamr.client.authentication.ApiKeyAuthenticationMethod;
import com.streamr.client.options.EncryptionOptions;
import com.streamr.client.options.SigningOptions;
import com.streamr.client.options.StreamrClientOptions;
import com.streamr.client.rest.Stream;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MetricsStats extends EventsStats {
    private StreamrClient client;
    private String metricsStreamId;
    private Map<String, Object> lastPayload = new HashMap<>();
    private Stream stream;

    public MetricsStats(int intervalInSec, String metricsStreamId, String metricsApiKey,
                        String wsUrl, String restUrl) {
        super("Metrics statistics", intervalInSec);
        lastPayload.put("kbReadPerSec", 0.0);
        lastPayload.put("eventReadPerSec", 0L);
        this.metricsStreamId = metricsStreamId;
        StreamrClientOptions options = new StreamrClientOptions(new ApiKeyAuthenticationMethod(metricsApiKey),
                SigningOptions.getDefault(), EncryptionOptions.getDefault(), wsUrl, restUrl);
        client = new StreamrClient(options);
    }

    private Stream getStream() throws IOException {
        if (stream == null) {
            stream = client.getStream(metricsStreamId);
        }
        return stream;
    }

    @Override
    public void logReport(ReportResult reportResult) {
        Map<String, Object> payload;
        if (reportResult == null) {
            payload = lastPayload;
        } else {
            payload = new HashMap<>();
            payload.put("kbReadPerSec", reportResult.getKbReadPerSec());
            payload.put("eventReadPerSec", reportResult.getEventReadPerSec());
            lastPayload = payload;
        }
        try {
            client.publish(getStream(), payload);
        } catch (Exception e) {
            log.error("Exception while trying to publish metrics", e);
        }
    }
}
