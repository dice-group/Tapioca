/**
 * tapioca.analyzer - ${project.description}
 * Copyright © 2015 Data Science Group (DICE) (michael.roeder@uni-paderborn.de)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
/**
 * This file is part of tapioca.analyzer.
 *
 * tapioca.analyzer is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.analyzer is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.analyzer.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.analyzer.dump;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DumpLoadingTask extends DumpAnalyzingTask {

	private static final Logger LOGGER = LoggerFactory.getLogger(DumpLoadingTask.class);

	private static final String DOWNLOAD_FOLDER = "temp";
	private static final boolean ADD_FILE_ENDING_IF_MISSING = true;
	private static final String XML_CONTENT_TYPE = "application/xml";

	protected File downloadFolder;
	protected boolean loadingFinished = false;
	protected boolean addFileEndingIfMissing = ADD_FILE_ENDING_IF_MISSING;

	public DumpLoadingTask(String datasetURI, File outputFolder, String[] dumps) {
		this(datasetURI, outputFolder, dumps, null);
	}

	public DumpLoadingTask(String datasetURI, File outputFolder, String[] dumps, ExecutorService executor) {
		super(datasetURI, outputFolder, dumps, executor);
		downloadFolder = generateDownloadFolder(outputFolder);
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
		if (!downloadFolder.exists()) {
			downloadFolder.mkdirs();
		}
		try {
			File dumpFile;
			for (int i = 0; i < dumps.length; ++i) {
				dumpFile = new File(downloadFolder.getAbsolutePath() + File.separator + extractFileName(dumps[i]));
				if (!dumpFile.exists()) {
					LOGGER.info("Start loading dump \"" + dumps[i] + "\".");
					try {
						dumpFile = loadDump(dumps[i], dumpFile, client);
					} catch (Exception e) {
						throw new RuntimeException(
								"Exception while trying to download dump from \"" + dumps[i] + "\".", e);
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

	protected File loadDump(String uri, File dumpFile, HttpClient client) throws ClientProtocolException, IOException {
		HttpGet hget = new HttpGet(uri);
		HttpResponse hres = client.execute(hget);

		if (hres.getStatusLine().getStatusCode() >= 300) {
			throw new RuntimeException("Wrong HTTP status: " + hres.getStatusLine().toString());
		} else {
			HttpEntity hen = hres.getEntity();
			if (addFileEndingIfMissing) {
				dumpFile = checkDumpFileEnding(hres, dumpFile);
			}
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
			return dumpFile;
		}
	}

	private File checkDumpFileEnding(HttpResponse hres, File dumpFile) {
		String filename = dumpFile.getName();
		boolean gzipped = false;
		if (filename.endsWith(".gz")) {
			filename = filename.substring(0, filename.length() - 3);
			gzipped = true;
		}
		Lang fileLanguage = RDFLanguages.resourceNameToLang(filename);
		Header header = hres.getFirstHeader("Content-Type");
		if (header == null) {
			LOGGER.warn("Couldn't get content type header from the response. Can't check dump file ending.");
			return dumpFile;
		}
		String contentType = header.getValue().split(";")[0];
		Lang responseLanguage = null;
		if (XML_CONTENT_TYPE.equalsIgnoreCase(contentType)) {
			responseLanguage = Lang.RDFXML;
		} else {
			responseLanguage = RDFLanguages.contentTypeToLang(header.getValue().split(";")[0]);
		}
		if (responseLanguage == null) {
			LOGGER.info("Got an unknown content type inside the response. Can't check dump file ending.");
			return dumpFile;
		}
		if ((fileLanguage == null) || (responseLanguage != fileLanguage)) {
			List<String> fileExtensions = responseLanguage.getFileExtensions();
			if ((fileExtensions != null) && (fileExtensions.size() > 0)) {
				filename += '.' + fileExtensions.get(0);
				LOGGER.info("Extended the dump file name from \"" + dumpFile.getName() + "\" to \"" + filename + "\".");
				return new File(dumpFile.getParentFile().getAbsolutePath() + File.separator
						+ (gzipped ? filename + ".gz" : filename));
			}
		}
		return dumpFile;
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

	public static String extractFileName(String uri) {
		String splits[] = uri.split("[/#]");
		String uriEnding = null;
		for (int i = splits.length - 1; (i >= 0) & (uriEnding == null); --i) {
			if (splits[i].length() > 0) {
				uriEnding = splits[i];
			}
		}
		if (uriEnding == null) {
			LOGGER.warn("Couldn't extract file name from \"" + uri + "\".");
			return "TEMP";
		}
		if (uriEnding.contains("?")) {
			if (uri.startsWith("?")) {
				return extractFileName(uri.substring(0, uri.length() - uriEnding.length()));
			} else {
				uriEnding = uriEnding.split("\\?")[0];
			}
		}
		return uriEnding;
	}

	public static File generateDownloadFolder(File outputFolder) {
		return new File(outputFolder.getAbsolutePath() + File.separator + DOWNLOAD_FOLDER);
	}
}
