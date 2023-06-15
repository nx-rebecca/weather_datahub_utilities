package uk.gov.metoffice.weatherdatahub.atmosphericorder.utils;

import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.time.Duration;

public class HttpUtils {

    private HttpUtils() {
    }

    public static HttpClient getHttpClient() {
        return HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.ALWAYS)
                .proxy(ProxySelector.of(new InetSocketAddress("webproxy-cloud.metoffice.gov.uk", 8082)))
                .build();
    }

    public static HttpRequest getHttpRequest(String accept, String URL, String clientID, String clientSecret) {
        return HttpRequest.newBuilder()
                .uri(URI.create(URL))
                .GET()
                .timeout(Duration.ofSeconds(20))
                .header("Accept", accept)
                .header("x-ibm-client-id", clientID)
                .header("x-ibm-client-secret", clientSecret)
                .build();
    }

    public static String getURL(String endpoint, String apiHost) {
        return "https://" + apiHost + "/1.0.0" + endpoint;
    }
}
