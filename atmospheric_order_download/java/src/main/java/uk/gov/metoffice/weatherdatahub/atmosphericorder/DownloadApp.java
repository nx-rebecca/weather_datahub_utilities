package uk.gov.metoffice.weatherdatahub.atmosphericorder;

import uk.gov.metoffice.weatherdatahub.atmosphericorder.impl.OrderDownloader;
import uk.gov.metoffice.weatherdatahub.atmosphericorder.service.FileDownloaderFactory;

import java.util.List;

public class DownloadApp {

    public static void main(String[] args) throws Exception {
        FileDownloaderFactory downloaderFactory = new FileDownloaderFactory();
        OrderDownloader download = new OrderDownloader("bab021d6506cd2d2a3a7573b41af8da8", "3d20b49ece1179bdbe91aff7d29ed3da", "o111719093773", "C:/Users/rebecca.southworth/wdh", 8);
        List<String> fileIds = download.getFileIdsforDownload(false);
        System.out.println(fileIds.toString());
        download.downloadFiles(fileIds);
    }


}
