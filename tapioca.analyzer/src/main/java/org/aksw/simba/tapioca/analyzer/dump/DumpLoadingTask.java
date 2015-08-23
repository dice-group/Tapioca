package org.aksw.simba.tapioca.analyzer.dump;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DumpLoadingTask extends DumpAnalyzingTask {

	private static final Logger LOGGER = LoggerFactory.getLogger(DumpLoadingTask.class);

	private static final String DOWNLOAD_FOLDER = "temp";

	protected File downloadFolder;
	protected boolean loadingFinished = false;

	public DumpLoadingTask(String datasetURI, File outputFolder, String[] dumps) {
		this(datasetURI, outputFolder, dumps, null);
	}

	public DumpLoadingTask(String datasetURI, File outputFolder, String[] dumps, ExecutorService executor) {
		super(datasetURI, outputFolder, dumps, executor);
		downloadFolder = new File(outputFolder.getAbsolutePath() + File.separator + DOWNLOAD_FOLDER);

	}

	@Override
	public void run() {
		loadingFinished = false;
		loadDumps();
		loadingFinished = true;
		super.run();
	}

	protected void loadDumps() {
		HttpClient client = new DefaultHttpClient();
		try {
			File dumpFile;
			for (int i = 0; i < dumps.length; ++i) {
				dumpFile = new File(downloadFolder.getAbsolutePath() + File.separator + extractFileName(dumps[i]));
				if (!dumpFile.exists()) {
					try {
						loadDump(dumps[i], dumpFile, client);
					} catch (Exception e) {
						throw new RuntimeException("Exception while trying to download dump from \"" + dumps[i] + "\".");
					}
				} else {
					LOGGER.info(dumpFile.getAbsolutePath() + " is already existing. It won't be downloaded.");
				}
				dumps[i] = dumpFile.getAbsolutePath();
			}
		} finally {
			HttpClientUtils.closeQuietly(client);
		}
	}

	protected void loadDump(String uri, File dumpFile, HttpClient client) throws ClientProtocolException, IOException {
		HttpGet hget = new HttpGet(uri);
		HttpResponse hres = client.execute(hget);

		if (hres.getStatusLine().getStatusCode() >= 300) {
			throw new RuntimeException("Wrong HTTP status: " + hres.getStatusLine().toString());
		} else {
			HttpEntity hen = hres.getEntity();
			InputStream is = null;
			FileOutputStream fout = null;
			try {
				is = hen.getContent();
				fout = new FileOutputStream(dumpFile);
				IOUtils.copy(is, fout);
			} finally {
				IOUtils.closeQuietly(is);
				IOUtils.closeQuietly(fout);
				EntityUtils.consume(hen);
			}
		}
	}

	@Override
	public String getId() {
		return "Loading/Analyzing(" + datasetURI + ' ' + Arrays.toString(dumps) + ")";
	}

	@Override
	public String getProgress() {
		if (loadingFinished) {
			return "Analyzing dumps...";
		} else {
			return "Loading dumps...";
		}
	}

	protected static String extractFileName(String uri) {
		String splits[] = uri.split("[/#]");
		for (int i = splits.length - 1; i >= 0; --i) {
			if (splits[i].length() > 0) {
				return splits[i];
			}
		}
		return "TEMP";
	}
}
