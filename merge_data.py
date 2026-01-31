"""
import pandas as pd
import sqlite3
orders=pd.read_csv("orders.csv")
users=pd.read_json("users.json")
conn=sqlite3.connect("restaurants.db")
restaurants=pd.read_sql("SELECT * FROM restaurants",conn)
final=orders.merge(users,on="user_id",how="left")
final=final.merge(restaurants,on="restaurant_id",how="left")
final.to_csv("final_food_delivery_dataset.csv",index=False)
print("Final dataset created successfully!!")


import sqlite3
import pandas as pd

conn = sqlite3.connect(":memory:")

with open("restaurants.sql", "r") as f:
    sql_script = f.read()

# Execute the script to create and populate the table
conn.executescript(sql_script)

tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)
print(tables)
restaurants = pd.read_sql("SELECT * FROM restaurants;", conn)
print(restaurants.head())
"""
import pandas as pd
import sqlite3
import json

# 1. Load orders.csv
orders = pd.read_csv("orders.csv")

# 2. Load users.json
with open("users.json", "r") as f:
    users_data = json.load(f)
users = pd.DataFrame(users_data)

# 3. Load restaurants.sql
conn = sqlite3.connect(":memory:")
with open("restaurants.sql", "r") as f:
    sql_script = f.read()
conn.executescript(sql_script)
restaurants = pd.read_sql("SELECT * FROM restaurants;", conn)

# 4. Merge datasets
merged = pd.merge(orders, users, on="user_id", how="left")
final_dataset = pd.merge(merged, restaurants, on="restaurant_id", how="left")
"""
# 5. Find city with highest revenue from Gold members
gold_members = final_dataset[final_dataset["membership"] == "Gold"]
city_revenue = gold_members.groupby("city")["total_amount"].sum().reset_index()
top_city = city_revenue.sort_values("total_amount", ascending=False).head(1)

print(top_city)
cuisine_aov = final_dataset.groupby("cuisine")["total_amount"].mean().reset_index() 
cuisine_aov = cuisine_aov.sort_values("total_amount", ascending=False) 
# Print full ranking 
print("Cuisine-wise Average Order Value:") 
print(cuisine_aov) 
# Print top cuisine 
top_cuisine = cuisine_aov.head(1) 
print("\nCuisine with Highest Average Order Value:") 
print(top_cuisine)

# Step 1: Calculate total spend per user
user_totals = final_dataset.groupby("user_id")["total_amount"].sum().reset_index()

# Step 2: Define spend buckets
def bucketize(amount):
    if amount < 500:
        return "<500"
    elif amount <= 1000:
        return "500-1000"
    elif amount <= 2000:
        return "1000-2000"
    else:
        return ">2000"

user_totals["spend_bucket"] = user_totals["total_amount"].apply(bucketize)

# Step 3: Count distinct users in each bucket
bucket_counts = user_totals.groupby("spend_bucket")["user_id"].nunique().reset_index()
print(bucket_counts)

# Merge orders with restaurants
merged = pd.merge(orders, restaurants, on="restaurant_id", how="left")
# Define rating ranges
def rating_bucket(rating):
    if rating < 3.5:
        return "<3.5"
    elif rating <= 4.0:
        return "3.5-4.0"
    elif rating <= 4.5:
        return "4.0-4.5"
    else:
        return ">4.5"
merged["rating_range"] = merged["rating"].apply(rating_bucket)

# Group by rating range and sum revenue
rating_revenue = merged.groupby("rating_range")["total_amount"].sum().reset_index()
rating_revenue = rating_revenue.sort_values("total_amount", ascending=False)

print("Revenue by Rating Range:")
print(rating_revenue)

print("\nHighest Revenue Rating Range:")
print(rating_revenue.head(1))

# Filter only Gold members
gold_orders = final_dataset[final_dataset["membership"] == "Gold"]
# Group by city and calculate average order value
city_aov = gold_orders.groupby("city")["total_amount"].mean().reset_index()
# Sort to find the top city
top_city = city_aov.sort_values("total_amount", ascending=False).head(1)
print(top_city)

# Merge orders with restaurants
merged = pd.merge(orders, restaurants, on="restaurant_id", how="left")

# Group by cuisine
cuisine_stats = merged.groupby("cuisine").agg(
    distinct_restaurants=("restaurant_id", "nunique"),
    total_revenue=("total_amount", "sum")
).reset_index()
# Sort: lowest restaurant count but high revenue
cuisine_stats = cuisine_stats.sort_values(["distinct_restaurants", "total_revenue"], ascending=[True, False])
print(cuisine_stats)

# Total orders
total_orders = len(final_dataset)
# Orders placed by Gold members
gold_orders = len(final_dataset[final_dataset["membership"] == "Gold"])
# Percentage (rounded)
gold_percentage = round((gold_orders / total_orders) * 100)
print(f"Percentage of orders placed by Gold members: {gold_percentage}%")

# Debug: check actual names
print(final_dataset["restaurant_name_y"].unique())
# Then filter correctly
target_restaurants = [
    "Grand Cafe Punjabi",
    "Grand Restaurant South Indian",
    "Ruchi Mess Multicuisine",
    "Ruchi Foods Chinese"
]
subset = final_dataset[final_dataset["restaurant_name_y"].isin(target_restaurants)]
stats = subset.groupby("restaurant_name_y").agg(
    total_orders=("order_id", "count"),
    avg_order_value=("total_amount", "mean")
).reset_index()
filtered = stats[stats["total_orders"] < 20]
top_restaurant = filtered.sort_values("avg_order_value", ascending=False).head(1)
print(top_restaurant)

# Merge orders with users and restaurants
merged = pd.merge(orders, users, on="user_id", how="left")
final_dataset = pd.merge(merged, restaurants, on="restaurant_id", how="left")
# Filter for the four combinations
subset = final_dataset[
    ((final_dataset["membership"] == "Gold") & (final_dataset["cuisine"].isin(["Indian","Italian"]))) |
    ((final_dataset["membership"] == "Regular") & (final_dataset["cuisine"].isin(["Indian","Chinese"])))
]
# Group by membership + cuisine
combo_revenue = subset.groupby(["membership","cuisine"])["total_amount"].sum().reset_index()
# Find highest revenue combination
top_combo = combo_revenue.sort_values("total_amount", ascending=False).head(1)
print(combo_revenue)
print("\nHighest Revenue Combination:")
print(top_combo)

# Convert order_date to datetime
final_dataset["order_date"] = pd.to_datetime(final_dataset["order_date"])

# Extract quarter
final_dataset["quarter"] = final_dataset["order_date"].dt.quarter

# Map quarter numbers to labels
quarter_map = {1: "Q1 (Jan–Mar)", 2: "Q2 (Apr–Jun)", 3: "Q3 (Jul–Sep)", 4: "Q4 (Oct–Dec)"}
final_dataset["quarter"] = final_dataset["quarter"].map(quarter_map)

# Group by quarter and sum revenue
quarter_revenue = final_dataset.groupby("quarter")["total_amount"].sum().reset_index()
quarter_revenue = quarter_revenue.sort_values("total_amount", ascending=False)
print("Revenue by Quarter:")
print(quarter_revenue)
print("\nQuarter with Highest Revenue:")
print(quarter_revenue.head(1))

# Filter for Gold members
gold_orders = final_dataset[final_dataset["membership"] == "Gold"]

# Count total orders
total_gold_orders = gold_orders["order_id"].count()
print(f"Total orders placed by Gold members: {total_gold_orders}")
# Filter for Hyderabad city
hyderabad_orders = final_dataset[final_dataset["city"] == "Hyderabad"]

# Sum total revenue and round
hyderabad_revenue = round(hyderabad_orders["total_amount"].sum())
print(f"Total revenue from Hyderabad city: {hyderabad_revenue}")

# Count distinct users who placed at least one order
distinct_users = final_dataset["user_id"].nunique()
print(f"Distinct users who placed at least one order: {distinct_users}")

# Filter for Gold members
gold_orders = final_dataset[final_dataset["membership"] == "Gold"]

# Calculate average order value (rounded to 2 decimals)
gold_aov = round(gold_orders["total_amount"].mean(), 2)
print(f"Average Order Value for Gold members: {gold_aov}")

# Filter for restaurants with rating >= 4.5
high_rating_orders = final_dataset[final_dataset["rating"] >= 4.5]

# Count total orders
total_high_rating_orders = high_rating_orders["order_id"].count()
print(f"Total orders for restaurants with rating >= 4.5: {total_high_rating_orders}")

# Filter only Gold members
gold_orders = final_dataset[final_dataset["membership"] == "Gold"]

# Group by city: total revenue + total orders
gold_city_stats = gold_orders.groupby("city").agg(
    total_revenue=("total_amount", "sum"),
    total_orders=("order_id", "count")
).reset_index()

# Find the top revenue city
top_city = gold_city_stats.sort_values("total_revenue", ascending=False).head(1)

print("Top revenue city among Gold members:")
print(top_city[["city", "total_orders"]])

# Total number of rows in the final merged dataset
row_count = len(final_dataset)
print(f"Total number of rows in the final merged dataset: {row_count}")
print(final_dataset.shape[0])
"""
print(pd.merge(orders, users, on="user_id", how="left"))

















