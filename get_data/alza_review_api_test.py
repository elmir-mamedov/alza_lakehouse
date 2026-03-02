from curl_cffi.requests import Session
import csv

commodity_id = 12409309
limit = 100
offset = 0
all_reviews = []

with Session() as session:
    while True:
        url = f"https://webapi.alza.cz/api/catalog/v2/commodities/{commodity_id}/reviews?country=cz&limit={limit}&offset={offset}"
        data = session.get(url, impersonate="chrome120").json()

        for review in data["value"]:
            for text in review["positives"]:
                all_reviews.append({"text": text, "label": 1})
            for text in review["negatives"]:
                all_reviews.append({"text": text, "label": 0})

        offset += limit
        if offset >= data["paging"]["size"]:
            break

print(f"Total reviews: {len(all_reviews)}")

with open("reviews.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["text", "label"])
    writer.writeheader()
    writer.writerows(all_reviews)

print("Saved to reviews.csv")