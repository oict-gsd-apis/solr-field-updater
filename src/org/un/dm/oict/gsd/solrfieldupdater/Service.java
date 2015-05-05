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
@SuppressWarnings("deprecation")
public class Service {

	/**
	 * Entry method to the application
	 * @param args
	 */
	public static void main(String[] args) {
		
		// Variables required to connect to the server
		String csvFilename = "";
		String solrServerUrl = "";
		String solrCollection = "";
		String solrUsername = "";
		String solrPassword = "";
		
		// Setup and connect to the server - auth based
		HttpSolrServer solrServer = connectSolrServer(solrServerUrl, solrCollection, solrUsername, solrPassword);
		
		// Setup the CSVReader
		CSVReader reader = readCSV(csvFilename);
		
		// Iterate through the documents and call the update method
		iterateDocuments(reader, solrServer);
	
		// Commit all the transactions
	    commitTransactions(solrServer);
	    
	    // Close the connection to the server
	    closeServer(solrServer);
	}
	
	/**
	 * This method is used to iterate through the rows of the CSV file and pass to the update
	 * method
	 * @param reader
	 * @param solrServer
	 */
	static void iterateDocuments(CSVReader reader, HttpSolrServer solrServer) {
		String [] nextLine;
	    try {
	    	int count = 0;
	    	// Read each line
			while ((nextLine = reader.readNext()) != null) {
				// Skip header row
				if (count > 0) {
					// Extract the id as Col 1 and url as Col2
					String id = nextLine[0];
					String url = nextLine[1];
					// Call the update method
					updateDocument(solrServer, "id", "url", id, url, count);
				}
				// Increment the count
				count++;
			}
	    } catch (Exception e) {
	    	System.out.println(e.getMessage());
	    }
	}
	
	/**
	 * This method is used to connect to the solr server using preemptive auth
	 * @param solrServerUrl
	 * @param solrCollection
	 * @param solrUsername
	 * @param solrPassword
	 * @return
	 */
	static HttpSolrServer connectSolrServer(String solrServerUrl, String solrCollection, String solrUsername, String solrPassword) {
		DefaultHttpClient httpclient = new DefaultHttpClient();
		httpclient.addRequestInterceptor(new PreemptiveAuthInterceptor(), 0);
		httpclient.getCredentialsProvider().setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(solrUsername, solrPassword));
		HttpSolrServer solrServer = new HttpSolrServer(solrServerUrl + solrCollection, httpclient);
		return solrServer;
	}
	
	/**
	 * This method is used to commit the transactions after finishing
	 * @param solrServer
	 */
	static void commitTransactions(HttpSolrServer solrServer) {
		try {
			solrServer.commit();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}		
	}
	
	/**
	 * This method is used to close the connection to the solr server
	 * @param solrServer
	 */
	static void closeServer(HttpSolrServer solrServer) {
		try {
			solrServer.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}		
	}
	
	/**
	 * This method is used to create and setup the CSV connection
	 * @param filename
	 * @return
	 */
	static CSVReader readCSV(String filename) {
		CSVReader reader = null;
		try {
			reader = new CSVReader(new FileReader(filename));
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return reader;
	}
		
	/**
	 * This method is used to call the Solr collection and update the corresponding field
	 * @param solr
	 * @param idFieldName
	 * @param updateFieldName
	 * @param id
	 * @param updateField
	 * @param count
	 */
	static void updateDocument(HttpSolrServer solr, String idFieldName, String updateFieldName, String id, String updateField, int count) {
		SolrInputDocument doc = new SolrInputDocument();
		Map<String, String> partialUpdate = new HashMap<String, String>();
		if (updateFieldName.equals("url"))
			updateField = cleanseUrl(updateField);
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
	
	/**
	 * This method is used simply to cleanse a url for specific characters
	 * @param url
	 * @return
	 */
	private static String cleanseUrl(String url) {
		String updateField = url.replaceAll("amp;", "");
		return updateField;
	}
}

/**
 * @author Kevin T Bradley
 * @dateCreated 01 September 2014
 * @description This class is used for preemptive authorization with an HTTP host
 */
@SuppressWarnings("deprecation")
class PreemptiveAuthInterceptor implements HttpRequestInterceptor {

	/**
	 * This method is used to set the preemp authorisation for an HTTP connection
	 */
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