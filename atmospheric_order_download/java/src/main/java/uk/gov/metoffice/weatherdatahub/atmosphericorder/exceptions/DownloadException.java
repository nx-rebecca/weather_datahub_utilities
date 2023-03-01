package uk.gov.metoffice.weatherdatahub.atmosphericorder.exceptions;

public class DownloadException extends Exception {
    public DownloadException(String message, Throwable cause) {
        super(message, cause);
    }

    public DownloadException(String message) {
        super(message);
    }
}
