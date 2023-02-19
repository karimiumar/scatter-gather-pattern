package com.umar.apps.scatter.gather.countdownlatch;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;



@SpringBootApplication
public class ScatterGatherCountdownLatchApp {
    
    private static final Logger logger = LoggerFactory.getLogger(ScatterGatherCountdownLatchApp.class);
    private static final ExecutorService threadPool = Executors.newFixedThreadPool(4);
    private static final List<Integer> prices = Collections.synchronizedList(new LinkedList<>());
    private static final List<Integer> productIds = Arrays.asList(1,2,3,4,5,6);
    
    public static void main(String[] args) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        SpringApplication.run(ScatterGatherCountdownLatchApp.class, args);
        productIds.stream().forEach(productId -> getPrice(productId));
        System.out.printf("Net Price of Products Queried:{%d} is {%d}%n", prices.size(), netPriceOfProducts());
        long endTime = System.currentTimeMillis();
        System.out.printf("Total time take to execute:{%d}%n", endTime - startTime);
        threadPool.shutdown();
    }
    
    private static void getPrice(Integer productId) {
        try {
            CountDownLatch latch = new CountDownLatch(3);
            threadPool.submit(new ProductTask("http://localhost:8080/products/price", productId, latch, prices));
            threadPool.submit(new ProductTask("http://localhost:8090/products/price", productId, latch, prices));
            threadPool.submit(new ProductTask("http://localhost:9080/products/price", productId, latch, prices));
            latch.await(1, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            logger.error("Exception occurred:{}", ex);
        }
    }
    
    private static Integer netPriceOfProducts() {
        return prices.stream().reduce(0, Integer::sum);
    }
    
    record ProductTask(String url, Integer productId, CountDownLatch latch, List<Integer> prices) implements Runnable {

        @Override
        public void run() {
            int price = getProductPrice(url, productId);
            prices.add(price);
            latch.countDown();
            logger.info("Prices size:{} --- CountDownLatch.getCount():{}", prices.size(), latch.getCount());
        }
        
        private Integer getProductPrice(String url, Integer productId) {
            HttpRequest request = HttpRequest.newBuilder(URI.create(url+"/"+productId)).GET().build();
            HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(20)).build();
            try {
                HttpResponse<String> resp = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if(Objects.nonNull(resp.body()) && !resp.body().isBlank()) {
                    int price = Integer.parseInt(resp.body());
                    logger.info("Found price:{} for productId:{} in url:{}",price, productId, url);
                    return price;
                }
            } catch (IOException | InterruptedException ex) {
                logger.error("Exception occurred:{}", ex);
            }
            return 0;
        }
        
    }
}
