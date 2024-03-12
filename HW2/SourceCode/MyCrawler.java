import com.opencsv.CSVWriter;
import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class MyCrawler extends WebCrawler {

    private static final Pattern docPatterns = Pattern.compile(".*(\\.(html?|php|pdf))(\\?|$)");
    private static final Pattern imgPatterns = Pattern.compile(".*(\\.(bmp|gif|jpe?g|ico|png|tiff?))(\\?|$)");
    private static final Pattern noExtensionFilters = Pattern.compile(".*(\\.[a-zA-Z]*)(\\?|$)");



    private static int fetchAttempted = 0;
    private static int totalUrls = 0;
    private static int fetchSucceeded = 0;
    private static int fetchFailedOrAborted = 0;
//    private static int uniqueUrlsInside = 0;
//    private static int uniqueUrlsOutside = 0;

    private static Set<String> uniqueUrls = new HashSet<>();
    private static Set<String> uniqueUrlsInside = new HashSet<>();
    private static Set<String> uniqueUrlsOutside = new HashSet<>();
    
    // Status codes and content types
    private static Map<Integer, Integer> statusCodes = new HashMap<>();
    private static Map<String, Integer> contentTypes = new HashMap<>();
  
    
    private static CSVWriter fetchWriter;
    private static CSVWriter visitWriter;
    private static CSVWriter urlsWriter;
    
    private static boolean fetchWriterClosed = false;
    private static boolean visitWriterClosed = false;
    private static boolean urlsWriterClosed = false;
   
    public MyCrawler() throws Exception{
    
     // Initialize fetchWriter
        File fetchFile = new File("./fetch_latimes.csv");
        boolean isFileNewOrEmpty1 = !fetchFile.exists() || fetchFile.length() == 0;
        fetchWriter= new CSVWriter(new FileWriter(fetchFile, true)); // Append mode

        // Write headers if the file is new or empty
        if (isFileNewOrEmpty1) {
            String[] fetchHeaders = {"URL", "Status"};
            fetchWriter.writeNext(fetchHeaders);
        }
        
        
        // Initialize visitWriter
        File visitFile = new File("./visit_latimes.csv");
        boolean isFileNewOrEmpty = !visitFile.exists() || visitFile.length() == 0;
        visitWriter = new CSVWriter(new FileWriter(visitFile, true)); // Append mode

        // Write headers if the file is new or empty
        if (isFileNewOrEmpty) {
            String[] visitHeaders = {"URL", "Size", "# of Outlinks", "Content-Type"};
            visitWriter.writeNext(visitHeaders);
        }
        urlsWriter = new CSVWriter(new FileWriter("./urls_latimes.csv"));
        
      
    }



   public boolean shouldVisit(Page referringPage, WebURL url) {
	    String href = url.getURL().toLowerCase();
	    uniqueUrls.add(href); // Track all URLs encountered
	    
	    // Prepare the row entry for urlsWriter with URL
	    String[] rowStrings = new String[]{href, null}; 
	    
	    // Check if the URL is internal or external
	    if(!(href.startsWith("https://www.latimes.com/") || href.startsWith("http://www.latimes.com/"))) {
	        uniqueUrlsOutside.add(href);
	        rowStrings[1] = "N_OK"; // Mark as external
	        urlsWriter.writeNext(rowStrings); // Log the external URL status
	        return false; // Do not visit external URLs
	    }
	    
	    // For internal URLs
	    uniqueUrlsInside.add(href);
	    rowStrings[1] = "OK"; // Mark as internal
	    urlsWriter.writeNext(rowStrings); // Log the internal URL status

	    // Further logic to decide whether to actually fetch the URL
	    if(docPatterns.matcher(href).matches() || imgPatterns.matcher(href).matches()) {
	        fetchAttempted++;
	        return true; // Fetch if it matches document or image patterns
	    }

	    if(href.contains("/xml/")) {
	        return false; // Do not fetch URLs pointing to XML resources
	    }

	    if(!noExtensionFilters.matcher(href).lookingAt()) {
	        fetchAttempted++;
	        return true; // Fetch if it does not match the noExtensionFilters pattern
	    }

	    return false; // Default case: do not fetch the URL
	}

    @Override
    protected void handlePageStatusCode(WebURL webUrl, int statusCode, String statusDescription) {

        String url = webUrl.getURL();
        url = url.replace(',','-');
        String [] rowStrings = {url, String.valueOf(statusCode)};
        fetchWriter.writeNext(rowStrings);
        
        fetchAttempted++;
        if(statusCode >= 200 && statusCode < 300) {
            fetchSucceeded++;
        } else {
            fetchFailedOrAborted++;
        }
        
        statusCodes.merge(statusCode, 1, Integer::sum);
    }
    
    @Override
    public void visit(Page page) {
        String url = page.getWebURL().getURL();
      
        String contentType = page.getContentType();
        if(contentType.indexOf(';')>0) {
            contentType = contentType.substring(0,contentType.indexOf(';'));
        }

        int fileSize = page.getContentData().length;

        int outLinks = 0;
        if (page.getParseData() instanceof HtmlParseData) {
            HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
            Set<WebURL> links = htmlParseData.getOutgoingUrls();
            outLinks = links.size();
        }

        totalUrls+=outLinks;

        String [] rowStrings = {url, fileSize + " Byte", String.valueOf(outLinks), contentType};
        visitWriter.writeNext(rowStrings);
        
        contentTypes.merge(contentType, 1, Integer::sum);
    }

    @Override
    public void onBeforeExit() {
        super.onBeforeExit();
        // Synchronize the closing process to avoid concurrent modification issues
        synchronized (MyCrawler.class) {
            try {
                if (!fetchWriterClosed) {
                    fetchWriter.close();
                    fetchWriterClosed = true; // Mark the writer as closed
                }
                if (!visitWriterClosed) {
                    visitWriter.close();
                    visitWriterClosed = true; // Mark the writer as closed
                }
                if (!urlsWriterClosed) {
                    urlsWriter.close();
                    urlsWriterClosed = true; // Mark the writer as closed
                }
            } catch (IOException e) {
                e.printStackTrace();
            }}
        }
//        private void generateCrawlReport() {
//            try (FileWriter writer = new FileWriter("./CrawlerReport2.txt")) {
//                writer.write("Fetch Statistics:\n");
//                writer.write("# fetches attempted: " + fetchAttempted + "\n");
//                writer.write("# fetches succeeded: " + fetchSucceeded + "\n");
//                writer.write("# fetches failed or aborted: " + fetchFailedOrAborted + "\n\n");
//
//                writer.write("Outgoing URLs:\n");
//                writer.write("Total URLs extracted: " + totalUrls + "\n");
//                writer.write("# unique URLs extracted: " + uniqueUrls.size() + "\n");
//                writer.write("# unique URLs within your news website: " + uniqueUrlsInside.size() + "\n");
//                writer.write("# unique URLs outside the news website: " + uniqueUrlsOutside.size() + "\n\n");
//
//                writer.write("Status Codes:\n");
//                for (Map.Entry<Integer, Integer> entry : statusCodes.entrySet()) {
//                    writer.write(entry.getKey() + ": " + entry.getValue() + "\n");
//                }
//                writer.write("\n");
//
//                writer.write("Content Types:\n");
//                for (Map.Entry<String, Integer> entry : contentTypes.entrySet()) {
//                    writer.write(entry.getKey() + ": " + entry.getValue() + "\n");
//                }
//                writer.write("\n");
//                
//                // Assuming file size statistics and other specific analyses might be implemented similarly
//                // Additional sections can be added here based on the collected data
//
//            } catch (IOException e) {
//                System.err.println("Error writing the crawl report: " + e.getMessage());
//            }
//        
        
        
        
}