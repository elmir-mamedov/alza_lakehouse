"""
Sentiment Analysis Pipeline
Uses the HuggingFace Inference API to analyze text sentiment,
then processes and saves the results with pandas.

Setup:
    1. Go to https://huggingface.co and create a free account
    2. Go to Settings > Access Tokens > New Token (read access is enough)
    3. Paste your token below (or set it as an environment variable)
"""

import requests
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv

# ── CONFIG ──────────────────────────────────────────────────────────────────

load_dotenv()
HF_TOKEN = os.getenv("HF_TOKEN")

# We use distilbert — a lightweight, free model hosted by HuggingFace
API_URL = "https://router.huggingface.co/hf-inference/models/distilbert/distilbert-base-uncased-finetuned-sst-2-english"
HEADERS = {"Authorization": f"Bearer {HF_TOKEN}"}

# ── SAMPLE DATA ──────────────────────────────────────────────────────────────
# These are fake customer reviews. You can replace with anything you like.

reviews = [
    # App Store — mobile app
    {"id": 1,  "source": "App Store",  "text": "Absolutely love this app, it changed my life!"},
    {"id": 2,  "source": "App Store",  "text": "Crashes every time I open it. Total garbage."},
    {"id": 3,  "source": "App Store",  "text": "It's okay, nothing special but gets the job done."},
    {"id": 4,  "source": "App Store",  "text": "The latest update completely broke everything. Very disappointed."},
    {"id": 5,  "source": "App Store",  "text": "Simple, clean, and does exactly what it promises. Five stars."},
    {"id": 6,  "source": "App Store",  "text": "Why did they remove the dark mode? That was the best feature!"},
    {"id": 7,  "source": "App Store",  "text": "Been using this for 2 years and it just keeps getting better."},

    # Play Store — mobile app
    {"id": 8,  "source": "Play Store", "text": "Fast, reliable, and the UI is beautiful."},
    {"id": 9,  "source": "Play Store", "text": "Terrible customer support. Never buying again."},
    {"id": 10, "source": "Play Store", "text": "Good value for money, would recommend."},
    {"id": 11, "source": "Play Store", "text": "Ads every 30 seconds. Absolutely unacceptable."},
    {"id": 12, "source": "Play Store", "text": "Smooth experience, no bugs, loads instantly. Impressive."},

    # Amazon — physical product (headphones)
    {"id": 13, "source": "Amazon",     "text": "Sound quality is incredible for the price. Very happy with this purchase."},
    {"id": 14, "source": "Amazon",     "text": "Broke after two weeks. Cheap plastic, poor build quality."},
    {"id": 15, "source": "Amazon",     "text": "Decent headphones but the ear cushions are a bit uncomfortable."},
    {"id": 16, "source": "Amazon",     "text": "Arrived damaged and customer service was no help at all."},
    {"id": 17, "source": "Amazon",     "text": "Best headphones I have ever owned. Crystal clear audio and great battery life."},
    {"id": 18, "source": "Amazon",     "text": "Not bad, but I expected more bass for this price range."},

    # Twitter — restaurant
    {"id": 19, "source": "Twitter",    "text": "Can't believe how bad this product has gotten lately."},
    {"id": 20, "source": "Twitter",    "text": "Just tried it for the first time — honestly impressed!"},
    {"id": 21, "source": "Twitter",    "text": "Waited an hour for cold food. Never again."},
    {"id": 22, "source": "Twitter",    "text": "The new menu is absolutely amazing, every dish was perfect."},
    {"id": 23, "source": "Twitter",    "text": "Pretty average honestly. Nothing I would go out of my way for."},

    # Reddit — laptop
    {"id": 24, "source": "Reddit",     "text": "Meh. Expected more based on the hype."},
    {"id": 25, "source": "Reddit",     "text": "This is genuinely the best tool in its category."},
    {"id": 26, "source": "Reddit",     "text": "Thermal throttling is a serious issue. Gets unbearably hot under load."},
    {"id": 27, "source": "Reddit",     "text": "Keyboard feel is fantastic and the display is gorgeous. Worth every penny."},
    {"id": 28, "source": "Reddit",     "text": "Battery lasts maybe 4 hours max. The spec sheet said 10. False advertising."},

    # Trustpilot — online service
    {"id": 29, "source": "Trustpilot", "text": "Seamless onboarding, responsive support, and a great product overall."},
    {"id": 30, "source": "Trustpilot", "text": "They charged me twice and it took 3 weeks to get a refund. Horrible experience."},
]

# ── API CALL ─────────────────────────────────────────────────────────────────

def analyze_sentiment(text: str) -> dict:
    """Send a single text to HuggingFace API and return the top sentiment result."""
    response = requests.post(API_URL, headers=HEADERS, json={"inputs": text})
    response.raise_for_status()  # raises an error if something went wrong

    # API returns a nested list: [[{label, score}, {label, score}]]
    results = response.json()[0]

    # Pick the label with the highest confidence score
    top = max(results, key=lambda x: x["score"])
    return {"sentiment": top["label"], "confidence": round(top["score"], 4)}


# ── PIPELINE ─────────────────────────────────────────────────────────────────

def run_pipeline(reviews: list) -> pd.DataFrame:
    """Run the full pipeline: fetch → parse → transform → return DataFrame."""
    print(f"Analyzing {len(reviews)} reviews...\n")
    rows = []

    for review in reviews:
        print(f"  [{review['id']:02d}] {review['text'][:50]}...")
        result = analyze_sentiment(review["text"])

        rows.append({
            "id":         review["id"],
            "source":     review["source"],
            "text":       review["text"],
            "sentiment":  result["sentiment"],
            "confidence": result["confidence"],
            "analyzed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

    return pd.DataFrame(rows)


# ── ANALYSIS ─────────────────────────────────────────────────────────────────

def print_summary(df: pd.DataFrame):
    """Print a simple summary of the results."""
    print("\n" + "="*55)
    print("RESULTS SUMMARY")
    print("="*55)

    # Overall sentiment counts
    counts = df["sentiment"].value_counts()
    print(f"\nOverall sentiment breakdown:")
    for label, count in counts.items():
        pct = count / len(df) * 100
        print(f"  {label:<10} {count} reviews  ({pct:.0f}%)")

    # Breakdown by source
    print(f"\nSentiment by source:")
    pivot = df.groupby(["source", "sentiment"]).size().unstack(fill_value=0)
    print(pivot.to_string())

    # Most confident predictions
    print(f"\nTop 3 most confident predictions:")
    top3 = df.nlargest(3, "confidence")[["text", "sentiment", "confidence"]]
    for _, row in top3.iterrows():
        print(f"  [{row['sentiment']}] {row['text'][:45]}... ({row['confidence']})")

    print("="*55)


# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # 1. Run the pipeline
    df = run_pipeline(reviews)

    # 2. Print summary to console
    print_summary(df)

    # 3. Save full results to CSV
    output_file = "sentiment_results.csv"
    df.to_csv(output_file, index=False)
    print(f"\nFull results saved to: {output_file}")
    print("\nDone!")