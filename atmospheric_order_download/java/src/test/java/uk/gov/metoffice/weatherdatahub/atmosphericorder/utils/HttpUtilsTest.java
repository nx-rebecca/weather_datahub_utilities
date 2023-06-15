package uk.gov.metoffice.weatherdatahub.atmosphericorder.utils;

import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HttpUtilsTest {

    private static String ORDER_ID = "order123";
    private static String API_HOST = "test.url.com";
    private static String ACCEPT = "application/json";
    private static String CLIENT_ID = "abcde";
    private static String CLIENT_SECRET = "fghij";

    @Test
    public void testGetUrl() {
        //When
        String endpoint = "/orders/" + ORDER_ID + "latest";
        String URL = HttpUtils.getURL(endpoint, API_HOST);

        //Then

        assertEquals("https://test.url.com/1.0.0/orders/order123/latest", URL);

    }

    @Test
    public void testGetHttpRequest() {
        //Given
        HttpClient client = HttpUtils.getHttpClient();
        String URL = "https://test.url.com/1.0.0/orders/order123/latest";
        //When
        HttpRequest request = HttpUtils.getHttpRequest(ACCEPT, URL, CLIENT_ID, CLIENT_SECRET);


        //Then
        assertEquals("GET", request.method());
        assertEquals("application/json", request.headers().map());



    }
}
