package org.un.dm.oict.gsd.solrfieldupdater;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.AuthState;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.ExecutionContext;
import org.apache.http.protocol.HttpContext;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import au.com.bytecode.opencsv.CSVReader;

/**
 * @author Kevin T Bradley
 * @dateCreated 01 September 2014
 * @description This application is used to update a particular field given an id in the form of a csv
 * i.e. id, url in urls.csv. It will loop through the csv file and update each corresponding Solr documents
 * url.
 */
public class Service {

	public static void main(String[] args) {
		
		String csvFilename = "";
		String solrServerUrl = "";
		String solrCollection = "";
		String solrUsername = "";
		String solrPassword = "";
		
		DefaultHttpClient httpclient = new DefaultHttpClient();
		httpclient.addRequestInterceptor(new PreemptiveAuthInterceptor(), 0);
		httpclient.getCredentialsProvider().setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(solrUsername, solrPassword));
		HttpSolrServer solrServer = new HttpSolrServer(solrServerUrl + solrCollection, httpclient);
		
		CSVReader reader = null;
		try {
			reader = new CSVReader(new FileReader(csvFilename));
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		
		String [] nextLine;
	    try {
	    	int count = 0;
			while ((nextLine = reader.readNext()) != null) {
				if (count > 0) {
					String id = nextLine[0];
					String url = nextLine[1];
					updateDocument(solrServer, "id", "url", id, url, count);
				}
				count++;
			}
	    } catch (Exception e) {
	    	System.out.println(e.getMessage());
	    }
		
		try {
			solrServer.commit();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} 		
		
		try {
			solrServer.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
	
	static void updateDocument(HttpSolrServer solr, String idFieldName, String updateFieldName, String id, String updateField, int count) {
		SolrInputDocument doc = new SolrInputDocument();
		Map<String, String> partialUpdate = new HashMap<String, String>();
		partialUpdate.put("set", updateField);
		doc.addField(idFieldName, id);
		doc.addField(updateFieldName, partialUpdate);
		
		try {
			UpdateResponse response = solr.add(doc);
			if (response.getStatus() == 0) {
				System.out.println("Processed: (" + count + ") " + id);
			} else {
				System.out.println("PROBLEM: Status Code " + response.getStatus() + " - " + id);
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} 		
	}
	
}

class PreemptiveAuthInterceptor implements HttpRequestInterceptor {

    public void process(final HttpRequest request, final HttpContext context) throws HttpException, IOException {
        AuthState authState = (AuthState) context.getAttribute(ClientContext.TARGET_AUTH_STATE);

        if (authState.getAuthScheme() == null) {
            CredentialsProvider credsProvider = (CredentialsProvider) context.getAttribute(ClientContext.CREDS_PROVIDER);
            HttpHost targetHost = (HttpHost) context.getAttribute(ExecutionContext.HTTP_TARGET_HOST);
            Credentials creds = credsProvider.getCredentials(new AuthScope(targetHost.getHostName(), targetHost.getPort()));
            if (creds == null)
                throw new HttpException("No credentials for preemptive authentication");
            authState.setAuthScheme(new BasicScheme());
            authState.setCredentials(creds);
        }

    }

}