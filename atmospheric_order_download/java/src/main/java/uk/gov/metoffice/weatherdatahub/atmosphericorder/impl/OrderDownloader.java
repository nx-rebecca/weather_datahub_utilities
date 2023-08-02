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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
//    private static final String API_HOST =  "rgw.5878-e94b1c46.eu-gb.apiconnect.appdomain.cloud/metoffice-ci/ci";
    private String runDateTime;
    private final int workers;
    private String directoryPath;

    private static final Logger LOG = LoggerFactory.getLogger(OrderDownloader.class);

//    private static final Logger;

    private final String orderId;

    public OrderDownloader(String clientID, String clientSecret, String orderId, String directoryPath, int workers) {
        this.clientID = clientID;
        this.clientSecret = clientSecret;
        this.orderId = orderId;
        this.workers = workers;
        this.directoryPath = directoryPath;
    }

    public List getFileIdsforDownload(boolean downloadAll) throws DownloadException {

        List<String> fileIds = new ArrayList<>();

        try {
            JSONObject order = getOrder();
            String modelId = getModelIdFromOrder(order);
            System.out.println(modelId);
            HttpResponse<String> response = sendLatestRequest();

            if (response.statusCode() != 200) {
                throw new DownloadException("Request for file list failed. HttpStatus code: " + response.statusCode());
            }

            JSONObject responseBody = new JSONObject(response.body());
            JSONObject orderDetails = responseBody.getJSONObject("orderDetails");
            JSONArray files = new JSONArray(orderDetails.getString("files"));
            addFileIds(fileIds, files, downloadAll, modelId, order);


        } catch (IOException | InterruptedException | JSONException ex) {
            throw new DownloadException("Failed to retrieve file list: " + ex.getMessage(), ex.getCause());
        }

        LOG.info("Files to download: " + fileIds.size());

        return fileIds;
    }

    private void addFileIds(List<String> fileIds, JSONArray files, boolean downloadAll, String modelId, JSONObject order) throws JSONException, IOException, InterruptedException {

        if (downloadAll) {
            this.runDateTime = files.getJSONObject(0).getString("runDateTime").replace(":", "");
        } else {
            String latestRun = getLatestRun(modelId, order);
            this.runDateTime = latestRun;
        }

        for (int i = 0; i < files.length(); i++) {
            JSONObject file = files.getJSONObject(i);
            String fileId = file.getString("fileId");
            Pattern special = Pattern.compile("[+]");
            Matcher hasSpecial = special.matcher(fileId);
            if (downloadAll) {
                if (hasSpecial.find()) {
                    fileIds.add(fileId);
                }
            } else {
                if (hasSpecial.find() && file.getString("runDateTime").replace(":", "").equals(runDateTime)) {
                    fileIds.add(fileId);
            }
            }

        }
    }

    private String getLatestRun(String modelId, JSONObject order) throws IOException, InterruptedException, JSONException {
        HttpResponse<String> runsResponse = sendRunsRequest(modelId);
        JSONArray requiredRuns = getRequiredRunsFromOrder(order);
        JSONObject runsCompleted = new JSONObject(runsResponse.body());
        JSONArray runs = new JSONArray(runsCompleted.getString("completeRuns"));
        StringBuilder latestRun = new StringBuilder();
        for (int i = 0; i < runs.length(); i++) {
            JSONObject run = runs.getJSONObject(i);
            for (int j = 0; j < requiredRuns.length(); j++) {
                if (run.getString("run").equals(requiredRuns.getString(j))) {
                    break;
                } else if (j == requiredRuns.length() - 1) {
                    runs.remove(i);
                    i --;
                }
            }
        }
        for (int i = 0; i < runs.length(); i++) {
            JSONObject run = runs.getJSONObject(i);
            String runFilter = run.getString("runFilter");
            if (i != runs.length() - 1) {
                JSONObject nextRun = runs.getJSONObject(i + 1);
                String nextRunFilter = nextRun.getString("runFilter");
                if (Integer.parseInt(runFilter) > Integer.parseInt(nextRunFilter)) {
                    latestRun.append(run.getString("runDateTime").replace(":", ""));
                    break;
                }
            } else {
                latestRun.append(run.getString("runDateTime").replace(":", ""));
            }

        }
        return latestRun.toString();
    }

    private JSONObject getOrder() throws IOException, InterruptedException, JSONException {
        HttpResponse<String> response = sendOrdersRequest();
        JSONObject responseJSON = new JSONObject(response.body());
        JSONArray orders = new JSONArray(responseJSON.getString("orders"));
        for (int i = 0; i < orders.length(); i++) {
            JSONObject order = orders.getJSONObject(i);
            if (order.getString("orderId").equals(this.orderId)) {
                return order;
            }
        }
        return null;
    }

    private String getModelIdFromOrder(JSONObject order) throws JSONException {
        return order.getString("modelId");
    }

    private JSONArray getRequiredRunsFromOrder(JSONObject order) throws JSONException {
        return new JSONArray(order.getString("requiredLatestRuns"));
    }

    private HttpResponse<String> sendRunsRequest(String modelID) throws IOException, InterruptedException {
        String accept = "application/json";
        String endpoint = "/runs/" + modelID;

        final String URL = HttpUtils.getURL(endpoint, API_HOST);
        HttpClient client = HttpUtils.getHttpClient();
        HttpRequest request = HttpUtils.getHttpRequest(accept, URL, clientID, clientSecret);

        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    public void downloadFiles(List<String> fileIds) throws DownloadException {

        int retries = 0;
        int downloadedFiles = 0;
        List<String> errors = new ArrayList<String>();

        while (retries < 5) {
            ExecutorService executorService = null;
            try {

                String path = directoryPath + "/downloads/" + orderId + "/" + runDateTime;
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
                        } else if (pair.getValue() != 200) {
                            LOG.error("Download failed. Status code: {}", pair.getValue());
                        } else if (pair.getValue() == 200) {
                            downloadedFiles++;
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
                LOG.debug("Errors: {}", errors.size());
                retries++;
                fileIds = errors;
                errors.clear();
                LOG.warn("Downloaded {} files. Download errors : {}. Retrying.", downloadedFiles, fileIds.size());
                downloadedFiles = 0;
                try {
                    int backoff = (int) Math.random() * 1000000;
                    Thread.sleep(backoff);
                } catch (InterruptedException e) {
                    // Do nothing
                }
            } else {
                LOG.info("Downloaded {} files.", downloadedFiles);
                break;
            }
        }
    }

    private HttpResponse<String> sendLatestRequest()
            throws IOException, InterruptedException {

        String accept = "application/json";
        String endpoint = "/orders/" + this.orderId + "/latest";

        final String URL = HttpUtils.getURL(endpoint, API_HOST);
        HttpClient client = HttpUtils.getHttpClient();
        HttpRequest request = HttpUtils.getHttpRequest(accept, URL, clientID, clientSecret);

        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> sendOrdersRequest() throws IOException, InterruptedException {
        String accept = "application/json";
        String endpoint = "/orders";

        final String URL = HttpUtils.getURL(endpoint, API_HOST);
        HttpClient client = HttpUtils.getHttpClient();
        HttpRequest request = HttpUtils.getHttpRequest(accept, URL, clientID, clientSecret);

        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

}
