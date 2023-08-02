package uk.gov.metoffice.weatherdatahub.atmosphericorder;

import uk.gov.metoffice.weatherdatahub.atmosphericorder.impl.OrderDownloader;
import uk.gov.metoffice.weatherdatahub.atmosphericorder.service.FileDownloaderFactory;

import java.util.List;

public class DownloadApp {

    public static void main(String[] args) throws Exception {
        FileDownloaderFactory downloaderFactory = new FileDownloaderFactory();
        OrderDownloader download = new OrderDownloader("4b072dfb61291c413c9f3929f32f5524", "a446dd5481a6fe6dd142bc19156ac6d2", "o132722116870", "C:/Users/rebecca.southworth/wdh", 8);
        List<String> fileIds = download.getFileIdsforDownload(false);
        System.out.println(fileIds.toString());
        download.downloadFiles(fileIds);
    }


}

