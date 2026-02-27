from curl_cffi.requests import Session

commodity_id = 12541394
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
for r in all_reviews[:2000]:
    print(r)

for r in all_reviews:
    if 'Nutno déle vyzkoušet,' in r['text']:
        print(r)