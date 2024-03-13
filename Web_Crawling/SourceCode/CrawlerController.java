import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;


public class CrawlerController {

    public static void main(String[] args) throws Exception {
        String crawlStorageFolder = "./crawl1";
        int numberOfCrawlers = 16;
        int maxDepth=16;
        int fetch=20000;
        int delay=203;
        String Url="https://www.latimes.com/";
        CrawlConfig config = new CrawlConfig();
        config.setCrawlStorageFolder(crawlStorageFolder);

        /* basic configuration */
        config.setMaxDepthOfCrawling(maxDepth);
        config.setMaxPagesToFetch(fetch);
        config.setPolitenessDelay(delay);
        config.setIncludeBinaryContentInCrawling(true);

        /* Instantiate the controller for this crawl. */
        PageFetcher pageFetcher = new PageFetcher(config);
        RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
        RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
        CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);

    
        controller.addSeed(Url); //Add seed URL
     
        controller.start(MyCrawler.class, numberOfCrawlers); //Start the Crawl
    }
}