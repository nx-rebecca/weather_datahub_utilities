package uk.gov.metoffice.weatherdatahub.atmosphericorder.impl;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.metoffice.weatherdatahub.atmosphericorder.exceptions.DownloadException;
import uk.gov.metoffice.weatherdatahub.atmosphericorder.utils.HttpUtils;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Application that is responsible for retrieving
 * Atmospheric Model data from the Met Office
 *
 * @version 1.0
 */

public class OrderDownloader {

    private final String clientID;
    private final String clientSecret;
    private static final String API_HOST = "api-metoffice.apiconnect.ibmcloud.com";
    private String runDateTime;
    private final int workers;
    private String directoryPath;

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderDownloader.class);

//    private static final Logger;

    private final String orderId;

    public OrderDownloader(String clientID, String clientSecret, String orderId, String directoryPath, int workers) {
        this.clientID = clientID;
        this.clientSecret = clientSecret;
        this.orderId = orderId;
        this.workers = workers;
        this.directoryPath = directoryPath;
    }

    public List getFileIdsforDownload() throws DownloadException {

        List fileIds = new ArrayList<String>();

        try {
            HttpResponse<String> response = sendLatestRequest("application/json", "/latest");

            if (response.statusCode() != 200) {
                throw new DownloadException("Request for file list failed. HttpStatus code: " + response.statusCode());
            }

            JSONObject responseBody = new JSONObject(response.body());
            JSONObject order = responseBody.getJSONObject("orderDetails");
            JSONArray files = new JSONArray(order.getString("files"));
            this.runDateTime = files.getJSONObject(0).getString("runDateTime").replace(":", "");

            for (int i = 0; i < files.length(); i++) {
                fileIds.add(files.getJSONObject(i).getString("fileId"));
            }

        } catch (IOException | InterruptedException | JSONException ex) {
            throw new DownloadException("Failed to retrieve file list" + ex.getMessage(), ex.getCause());
        }

        return fileIds;
    }

    public void downloadFiles(List<String> fileIds) throws DownloadException {

        int retries = 0;
        List<String> errors = new ArrayList<String>();

        while (retries < 5) {
            ExecutorService executorService = null;
            if (retries > 0) {
                System.out.println("Retrying...");
            }
            try {

                String path = directoryPath + "/downloads/" + runDateTime;
                Files.createDirectories(Paths.get(path));

                executorService = Executors.newFixedThreadPool(workers);

                Collection downloaders = new ArrayList();

                for (int i = 0; i < fileIds.size(); i++) {
                    String fileID = fileIds.get(i);
                    downloaders.add(new FileDownloader(fileID, this.clientID, this.clientSecret, this.orderId, API_HOST, path));
                }

                List<Future<Map<String, Integer>>> results = executorService.invokeAll(downloaders);

                for (Future<Map<String, Integer>> result : results) {
                    Map<String, Integer> report = result.get(300, TimeUnit.SECONDS);
                    for (Map.Entry<String, Integer> pair : report.entrySet()) {
                        if (pair.getValue() >= 500) {
                            errors.add(pair.getKey());
                        }
                    }
                }
            } catch (IOException | InterruptedException | ExecutionException | TimeoutException e) {
                throw new DownloadException("Download failed: " + e.getMessage(), e.getCause());
            } finally {
                if (executorService != null) {
                    executorService.shutdown();
                }
            }

            if (errors.size() != 0) {
                retries++;
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    // Do nothing
                }
            } else break;
        }
    }

    public HttpResponse<String> sendLatestRequest(String accept, String endpoint)
            throws IOException, InterruptedException {

        final String URL = HttpUtils.getURL(endpoint, orderId, API_HOST);
        HttpClient client = HttpUtils.getHttpClient();
        HttpRequest request = HttpUtils.getHttpRequest(accept, URL, clientID, clientSecret);

        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

}
