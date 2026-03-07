# Alza sentiments
<img width="1035" height="354" alt="image" src="docs/wordcloud.png" />

#### Data & Ethics Note

--- Data was scraped from Alza.cz for personal research and model
training purposes only. No data is redistributed, published, or used commercially.
The scraper mimics respectful browsing behavior with randomized delays to avoid 
server load. ---

## TODO list

### Cross-variant Review Deduplication
Alza's product catalog follows a variant model where a single product
(e.g. a perfume) is listed as multiple distinct commodities differentiated
by attributes such as volume or color (e.g. 125ml, 60ml, 5ml). Each variant 
has a unique commodity_id
and a separate URL in the sitemap, but all variants share the same 
review pool — Alza aggregates reviews at the product family level, 
not the variant level.


As a result, iterating over url_queue by commodity_id causes the same 
reviews to be scraped and stored multiple times — once per variant — 
inflating the dataset with duplicate records and skewing any downstream
analysis or model training.
Solution: Deduplicate scraped reviews using a unique review identifier
returned by the API (review_id), ensuring each review is stored exactly 
once regardless of how many variants triggered its collection.

### Fine-tuning LLM
- take czech bert and run a test performance
on product reviews.
- fine tune it on reviews off similar product and then
run the test inference again.
- compare performance before and after fine-tuning

### Class-noise problem is still present
