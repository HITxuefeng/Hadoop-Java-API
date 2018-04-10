package com.iie.s3.demo;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

public class HttpConnectionUtil {

	public static void main(String[] args) {
		String urlAddress = "http://88.1.1.30/get/Home/ImageService?CommandType=GetImage&ContentType=application/dicom&ObjectUID=1.2.156.1.1.2017082308380632400.8";
		downloadFile(urlAddress, "d:\\xxxxx");
	}

	public static File downloadFile(String urlPath, String downloadDir) {
		File file = null;
		OutputStream bufferedOutputStream = null;
		InputStream inputStream = null;
		try {
			URL url = new URL(urlPath);
			String fileName = url.getQuery().split("ObjectUID=")[1];
			URLConnection urlConnection = url.openConnection();
			HttpURLConnection httpURLConnection = (HttpURLConnection) urlConnection;
			httpURLConnection.setRequestMethod("GET");
			httpURLConnection.setConnectTimeout(1000);
			httpURLConnection.setRequestProperty("Charset", "UTF-8");
			httpURLConnection.connect();
			inputStream = new BufferedInputStream(httpURLConnection.getInputStream());
			String path = downloadDir + File.separatorChar + fileName + ".dcm";
			file = new File(path);
			if (!file.getParentFile().exists()) {
				file.getParentFile().mkdirs();
			}
			bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(file));
			int size = 0;
			byte[] buf = new byte[1024];
			while ((size = inputStream.read(buf)) != -1) {
				bufferedOutputStream.write(buf, 0, size);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (inputStream != null) {
					inputStream.close();
				}
				if (bufferedOutputStream != null) {
					bufferedOutputStream.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return file;
	}


}