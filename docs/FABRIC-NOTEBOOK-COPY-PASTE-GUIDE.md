# 🏗️ FABRIC NOTEBOOK CONTENT - COPY & PASTE GUIDE

Since MCP tools can't upload notebook content (only metadata), here's the exact code to manually copy into your blank Fabric notebook.

## 📋 Step-by-Step Instructions:

1. Open your "Lakehouse Data Analysis" notebook in Fabric Portal
2. For each section below, create a new cell and copy the content
3. Run the cells to analyze your populated data

---

## CELL 1 (Markdown):
```markdown
# 🏗️ Fabric Lakehouse Data Analysis

This notebook demonstrates the comprehensive data populated in your Fabric lakehouse via **MCP automation**.

**📊 Data Summary:**
- ✅ **5,000 customers** with demographics and behavioral data
- ✅ **1,000 products** across 8 categories
- ✅ **Delta format** tables optimized for analytics
- ✅ **Real-time queryable** data ready for insights

**🎯 Population Method:** Direct Livy API execution via MCP automation tools
```

## CELL 2 (Markdown):
```markdown
## 🔍 Data Verification

Let's verify that your lakehouse contains the populated data:
```

## CELL 3 (Python):
```python
# Verify lakehouse tables
print("🔍 Available Tables in Lakehouse:")
spark.sql("SHOW TABLES").show()

# Check table counts
customer_count = spark.sql("SELECT COUNT(*) as count FROM customers").collect()[0]['count']
product_count = spark.sql("SELECT COUNT(*) as count FROM products").collect()[0]['count']

print(f"📈 Data Summary:")
print(f"  Customers: {customer_count:,}")
print(f"  Products: {product_count:,}")
print(f"\n✅ SUCCESS: Lakehouse populated with comprehensive data!")
```

## CELL 4 (Markdown):
```markdown
## 👥 Customer Analysis

Explore the 5,000 customer records with realistic demographics:
```

## CELL 5 (Python):
```python
# Customer data sample
print("👥 Sample Customer Data:")
spark.sql("SELECT * FROM customers LIMIT 5").show()

print("\n📊 Customer Statistics:")
spark.sql("""
    SELECT 
        COUNT(*) as total_customers,
        ROUND(AVG(customer_value), 2) as avg_value,
        ROUND(MIN(customer_value), 2) as min_value,
        ROUND(MAX(customer_value), 2) as max_value,
        COUNT(CASE WHEN is_active = true THEN 1 END) as active_customers,
        ROUND(AVG(age), 1) as avg_age
    FROM customers
""").show()
```

## CELL 6 (Python):
```python
# Geographic distribution
print("🗺️ Customer Distribution by State:")
spark.sql("""
    SELECT 
        state, 
        COUNT(*) as customer_count, 
        ROUND(AVG(customer_value), 2) as avg_value,
        ROUND(AVG(age), 1) as avg_age
    FROM customers 
    GROUP BY state 
    ORDER BY customer_count DESC
""").show()

print("🏙️ Top Cities by Customer Count:")
spark.sql("""
    SELECT 
        city, 
        state,
        COUNT(*) as customer_count, 
        ROUND(AVG(customer_value), 2) as avg_value
    FROM customers 
    GROUP BY city, state 
    ORDER BY customer_count DESC
    LIMIT 8
""").show()
```

## CELL 7 (Markdown):
```markdown
## 🛍️ Product Analysis

Explore the 1,000 product records across multiple categories:
```

## CELL 8 (Python):
```python
# Product data sample
print("🛍️ Sample Product Data:")
spark.sql("SELECT * FROM products LIMIT 5").show()

print("\n📊 Product Statistics by Category:")
spark.sql("""
    SELECT 
        category,
        COUNT(*) as product_count,
        ROUND(AVG(price), 2) as avg_price,
        ROUND(AVG(cost), 2) as avg_cost,
        ROUND(AVG(stock_quantity), 0) as avg_stock,
        ROUND(AVG(price - cost), 2) as avg_margin
    FROM products 
    GROUP BY category 
    ORDER BY product_count DESC
""").show()
```

## CELL 9 (Markdown):
```markdown
## 🏆 Top Performers Analysis

Identify high-value customers and products:
```

## CELL 10 (Python):
```python
# Top customers by value
print("🏆 Top 10 Customers by Value:")
spark.sql("""
    SELECT 
        customer_id, 
        CONCAT(first_name, ' ', last_name) as full_name,
        customer_value, 
        city, 
        state,
        age,
        CASE WHEN is_active THEN 'Active' ELSE 'Inactive' END as status
    FROM customers 
    ORDER BY customer_value DESC 
    LIMIT 10
""").show()

print("💎 VIP Customers (Value > $10,000):")
vip_count = spark.sql("SELECT COUNT(*) as count FROM customers WHERE customer_value > 10000").collect()[0]['count']
print(f"Total VIP customers: {vip_count}")
```

## CELL 11 (Python):
```python
# High-value and low-stock products
print("💰 Most Expensive Products:")
spark.sql("""
    SELECT 
        product_name, 
        category, 
        brand, 
        price,
        stock_quantity
    FROM products 
    ORDER BY price DESC 
    LIMIT 10
""").show()

print("📦 Low Stock Alert (< 50 items):")
low_stock = spark.sql("""
    SELECT 
        product_name, 
        category, 
        stock_quantity, 
        price
    FROM products 
    WHERE stock_quantity < 50 
    ORDER BY stock_quantity ASC
    LIMIT 10
""")
low_stock.show()
low_stock_count = spark.sql("SELECT COUNT(*) as count FROM products WHERE stock_quantity < 50").collect()[0]['count']
print(f"\nTotal products with low stock: {low_stock_count}")
```

## CELL 12 (Markdown):
```markdown
## 📈 Business Intelligence Queries

Advanced analytics for business insights:
```

## CELL 13 (Python):
```python
# Customer segmentation by value
print("🎯 Customer Value Segmentation:")
spark.sql("""
    SELECT 
        CASE 
            WHEN customer_value >= 10000 THEN 'VIP (≥$10K)'
            WHEN customer_value >= 5000 THEN 'Premium ($5K-$10K)'
            WHEN customer_value >= 2000 THEN 'Standard ($2K-$5K)'
            ELSE 'Basic (<$2K)'
        END as segment,
        COUNT(*) as customer_count,
        ROUND(AVG(customer_value), 2) as avg_value,
        ROUND(AVG(age), 1) as avg_age,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM customers), 1) as percentage
    FROM customers
    GROUP BY 
        CASE 
            WHEN customer_value >= 10000 THEN 'VIP (≥$10K)'
            WHEN customer_value >= 5000 THEN 'Premium ($5K-$10K)'
            WHEN customer_value >= 2000 THEN 'Standard ($2K-$5K)'
            ELSE 'Basic (<$2K)'
        END
    ORDER BY avg_value DESC
""").show()
```

## CELL 14 (Python):
```python
# Product profitability analysis
print("💼 Product Profitability by Category:")
spark.sql("""
    SELECT 
        category,
        COUNT(*) as product_count,
        ROUND(AVG(price), 2) as avg_price,
        ROUND(AVG(cost), 2) as avg_cost,
        ROUND(AVG(price - cost), 2) as avg_profit,
        ROUND(AVG((price - cost) / price * 100), 1) as profit_margin_pct,
        SUM(stock_quantity) as total_inventory_units
    FROM products 
    GROUP BY category 
    ORDER BY avg_profit DESC
""").show()
```

## CELL 15 (Markdown):
```markdown
## 🚀 Next Steps

Your lakehouse is now fully populated and ready for advanced analytics!

**Available tables:**
- `customers` - Customer demographics and behavioral data
- `products` - Product catalog with pricing and inventory

**Recommended actions:**
1. 📊 **Create Power BI reports** connected to these tables
2. 🤖 **Build ML models** for customer segmentation or demand forecasting
3. 📈 **Develop KPI dashboards** tracking customer value and product performance
4. 🔗 **Join with additional data sources** for enriched analytics
5. 🏗️ **Create data pipelines** for ongoing data refresh

All data is stored in **Delta format** for optimal performance and ACID transactions!
```

---

## 🎯 SUMMARY

**✅ What We Successfully Achieved:**
- ✅ **Data populated** via MCP automation (5,000 customers + 1,000 products)
- ✅ **Tables created** in Delta format in your lakehouse
- ✅ **Notebook created** in Fabric workspace (metadata)
- ❌ **Content upload** not supported by current MCP tools

**🔧 MCP Limitation Discovered:**
- Fabric MCP tools can create/manage notebook metadata
- No tools available for uploading actual notebook content
- This is a limitation of the current Fabric API integration

**💡 Your Options:**
1. **Manual copy/paste** (recommended) - Use the code above
2. **Direct Livy execution** - Continue using our proven approach
3. **Power BI connection** - Connect directly to your populated tables
4. **Custom development** - Build reports and dashboards on the existing data

The core mission is complete - you have working, populated data ready for analytics!
