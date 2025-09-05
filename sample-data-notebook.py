# Sample Data Creation Script for Fabric Lakehouse
# This notebook creates sample sales data in the MCPDemoLakehouse

# Cell 1: Import libraries and setup
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

print("🚀 Starting sample data creation for MCPDemoLakehouse")
print("Setting up Spark session...")

# Cell 2: Generate sample sales data
print("📊 Generating sample sales data...")

# Define sample data
products = ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones", "Webcam", "Speakers", "Tablet"]
regions = ["North America", "Europe", "Asia", "South America", "Africa"]
sales_channels = ["Online", "Retail", "Partner", "Direct"]

# Generate 100 sample sales records
sales_data = []
start_date = datetime(2024, 1, 1)

for i in range(100):
    sale_date = start_date + timedelta(days=random.randint(0, 365))
    sales_data.append({
        "sale_id": f"S{i+1:04d}",
        "product_name": random.choice(products),
        "region": random.choice(regions),
        "sales_channel": random.choice(sales_channels),
        "quantity": random.randint(1, 15),
        "unit_price": round(random.uniform(25.0, 1200.0), 2),
        "discount_percent": random.choice([0, 5, 10, 15, 20]),
        "sale_date": sale_date.strftime("%Y-%m-%d"),
        "customer_segment": random.choice(["Enterprise", "SMB", "Consumer"])
    })

print(f"Generated {len(sales_data)} sales records")

# Cell 3: Create DataFrame and add calculated columns
print("🔧 Creating DataFrame and calculating derived fields...")

# Create DataFrame
sales_df = spark.createDataFrame(sales_data)

# Add calculated columns
sales_df = sales_df.withColumn("gross_amount", col("quantity") * col("unit_price"))
sales_df = sales_df.withColumn("discount_amount", col("gross_amount") * col("discount_percent") / 100)
sales_df = sales_df.withColumn("net_amount", col("gross_amount") - col("discount_amount"))
sales_df = sales_df.withColumn("sale_year", year(to_date(col("sale_date"))))
sales_df = sales_df.withColumn("sale_month", month(to_date(col("sale_date"))))

print("DataFrame schema:")
sales_df.printSchema()

print("Sample data preview:")
sales_df.show(10, truncate=False)

# Cell 4: Save main sales table
print("💾 Saving sales data to demo_sales table...")

try:
    sales_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("demo_sales")
    print("✅ Successfully saved demo_sales table")
    print(f"Total records: {sales_df.count()}")
except Exception as e:
    print(f"❌ Error saving demo_sales table: {str(e)}")

# Cell 5: Create product performance summary
print("📈 Creating product performance summary...")

product_summary = sales_df.groupBy("product_name").agg(
    sum("quantity").alias("total_quantity_sold"),
    sum("net_amount").alias("total_revenue"),
    count("*").alias("number_of_sales"),
    avg("unit_price").alias("avg_unit_price"),
    avg("discount_percent").alias("avg_discount_percent")
).orderBy(desc("total_revenue"))

print("Product Performance Summary:")
product_summary.show(truncate=False)

try:
    product_summary.write.mode("overwrite").saveAsTable("product_performance_summary")
    print("✅ Successfully saved product_performance_summary table")
except Exception as e:
    print(f"❌ Error saving product summary: {str(e)}")

# Cell 6: Create regional sales analysis
print("🌍 Creating regional sales analysis...")

regional_summary = sales_df.groupBy("region", "sale_year").agg(
    sum("net_amount").alias("total_revenue"),
    sum("quantity").alias("total_units"),
    count("*").alias("total_sales"),
    avg("net_amount").alias("avg_sale_value")
).orderBy("region", "sale_year")

print("Regional Sales Analysis:")
regional_summary.show(truncate=False)

try:
    regional_summary.write.mode("overwrite").saveAsTable("regional_sales_analysis")
    print("✅ Successfully saved regional_sales_analysis table")
except Exception as e:
    print(f"❌ Error saving regional analysis: {str(e)}")

# Cell 7: Create monthly sales trends
print("📅 Creating monthly sales trends...")

monthly_trends = sales_df.groupBy("sale_year", "sale_month").agg(
    sum("net_amount").alias("monthly_revenue"),
    sum("quantity").alias("monthly_units"),
    count("*").alias("monthly_sales_count"),
    countDistinct("product_name").alias("unique_products_sold")
).orderBy("sale_year", "sale_month")

print("Monthly Sales Trends:")
monthly_trends.show(truncate=False)

try:
    monthly_trends.write.mode("overwrite").saveAsTable("monthly_sales_trends")
    print("✅ Successfully saved monthly_sales_trends table")
except Exception as e:
    print(f"❌ Error saving monthly trends: {str(e)}")

# Cell 8: Summary report
print("\n🎉 Sample Data Creation Complete!")
print("=" * 50)
print("Created the following tables in MCPDemoLakehouse:")
print("1. demo_sales - Main sales transaction data")
print("2. product_performance_summary - Product performance metrics")
print("3. regional_sales_analysis - Sales by region and year")
print("4. monthly_sales_trends - Monthly sales trend analysis")
print("\nYou can now query these tables using SQL endpoints or Spark SQL")
print("Example query: SELECT * FROM demo_sales LIMIT 10")
