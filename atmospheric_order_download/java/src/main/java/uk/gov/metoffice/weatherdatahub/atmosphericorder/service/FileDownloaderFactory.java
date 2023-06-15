package uk.gov.metoffice.weatherdatahub.atmosphericorder.service;

import uk.gov.metoffice.weatherdatahub.atmosphericorder.impl.FileDownloader;

public class FileDownloaderFactory {

    public FileDownloaderFactory() {
    }

    public FileDownloader createFileDownloader(String fileID, String clientId, String clientSecret, String orderId, String apiHost, String path) {
        return new FileDownloader(fileID, clientId, clientSecret, orderId, apiHost, path);
    }
}
