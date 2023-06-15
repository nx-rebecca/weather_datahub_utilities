package uk.gov.metoffice.weatherdatahub.atmosphericorder.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.metoffice.weatherdatahub.atmosphericorder.utils.HttpUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class FileDownloader implements Callable {

    private String fileID;
    private String clientId;
    private String clientSecret;
    private String orderId;
    private String apiHost;
    private String path;

    private static final Logger LOG = LoggerFactory.getLogger(FileDownloader.class);

    public FileDownloader(String fileID, String clientId, String clientSecret, String orderId, String apiHost, String path) {
        this.fileID = fileID;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.orderId = orderId;
        this.apiHost = apiHost;
        this.path = path;
    }

    @Override
    public Map<String, Integer> call() {

        String endpoint = "/orders/" + this.orderId + "/latest/" + this.fileID + "/data";

        final String URL = HttpUtils.getURL(endpoint, this.apiHost);

        HttpClient client = HttpUtils.getHttpClient();

        HttpRequest request = HttpUtils.getHttpRequest("application/x-grib", URL, this.clientId, this.clientSecret);

        String localGribFile = path + "/" + fileID + ".grib";

        Map<String, Integer> report = new HashMap<String, Integer>();

        try {
            HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());

            if (response.statusCode() == 200) {
                FileOutputStream fos = new FileOutputStream(localGribFile);
                fos.write(response.body());
                LOG.debug("GRIB file of " + fos.getChannel().size() + " bytes saved as " + localGribFile);
                report.put(fileID, response.statusCode());
            } else {
                LOG.warn("Status code: " + response.statusCode());
                report.put(fileID, response.statusCode());

            }
        } catch (IOException e) {
            //e.printStackTrace();
            LOG.warn("Download failed.", e);
            report.put(fileID, 600);
        } catch (InterruptedException e) {
//            e.printStackTrace();
            LOG.warn("Download failed.", e);
            report.put(fileID, 600);
        }


        return report;
    }

}
