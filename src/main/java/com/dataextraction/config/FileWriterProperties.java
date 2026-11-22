package com.dataextraction.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import com.dataextraction.sink.ExtractOutputFormat;

@ConfigurationProperties(prefix = "filewriter")
public class FileWriterProperties {

    private String basePath;
    private String environment;
    private String fileExtension;
    private ExtractOutputFormat outputFormat;

    public String getBasePath() {
        return basePath;
    }
    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }
    public String getEnvironment() {
        return environment;
    }
    public void setEnvironment(String environment) {
        this.environment = environment;
    }
    public String getFileExtension() {
        return fileExtension;
    }
    public void setFileExtension(String fileExtension) {
        this.fileExtension = fileExtension;
    }
    public ExtractOutputFormat getOutputFormat() {
        return outputFormat;
    }
    public void setOutputFormat(ExtractOutputFormat outputFormat) {
        this.outputFormat = outputFormat;
    }
}
