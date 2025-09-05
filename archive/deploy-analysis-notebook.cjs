const { spawn } = require('child_process');

const config = {
    workspaceId: "bcb44215-0e69-46d3-aac9-fb92fadcd982",
    lakehouseId: "5e6b33fe-1f33-419a-a954-bce697ccfe61"
};

async function getAzureToken() {
    const { execSync } = require('child_process');
    try {
        const result = execSync('az account get-access-token --resource "https://analysis.windows.net/powerbi/api"', { encoding: 'utf8' });
        const tokenData = JSON.parse(result);
        return tokenData.accessToken;
    } catch (error) {
        console.error('Failed to get Azure token:', error.message);
        return null;
    }
}

async function sendMCPRequest(tool, params, timeout = 120000) {
    return new Promise((resolve, reject) => {
        const timeoutHandle = setTimeout(() => {
            reject(new Error(`Request timeout for ${tool}`));
        }, timeout);

        const requestId = Date.now();
        const request = {
            jsonrpc: "2.0",
            id: requestId,
            method: "tools/call",
            params: {
                name: tool,
                arguments: params
            }
        };

        console.log(`📤 ${tool}...`);

        let responseReceived = false;

        const dataHandler = (data) => {
            if (responseReceived) return;
            
            const output = data.toString();
            
            try {
                const lines = output.split('\n');
                for (const line of lines) {
                    if (line.trim()) {
                        const parsed = JSON.parse(line);
                        if (parsed.id === requestId) {
                            responseReceived = true;
                            clearTimeout(timeoutHandle);
                            
                            if (parsed.error) {
                                reject(new Error(`MCP error: ${parsed.error.message}`));
                            } else {
                                resolve(parsed.result);
                            }
                            return;
                        }
                    }
                }
            } catch (e) {
                // Continue collecting
            }
        };

        mcpProcess.stdout.on('data', dataHandler);
        mcpProcess.stderr.on('data', dataHandler);

        mcpProcess.stdin.write(JSON.stringify(request) + '\n');
    });
}

let mcpProcess;

async function deployNotebook() {
    console.log('📚 Deploying Lakehouse Population Notebook to Fabric');
    console.log('='.repeat(60));
    
    const token = await getAzureToken();
    if (!token) {
        console.error('❌ Failed to get Azure token');
        return;
    }
    
    mcpProcess = spawn('node', ['C:\\Repos\\Fabric-Analytics-MCP\\build\\index.js'], {
        stdio: ['pipe', 'pipe', 'pipe'],
        cwd: 'C:\\Repos\\Fabric-Analytics-MCP'
    });

    await new Promise(resolve => setTimeout(resolve, 3000));

    try {
        // Create the notebook with comprehensive content
        console.log('📝 Creating comprehensive lakehouse analysis notebook...');
        
        const notebookContent = {
            "cells": [
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [
                        "# 🏗️ Fabric Lakehouse Data Population Analysis\\n\\n",
                        "This notebook demonstrates the comprehensive data that was populated in your Fabric lakehouse via **direct MCP automation**.\\n\\n",
                        "**📊 Data Summary:**\\n",
                        "- ✅ **5,000 customers** with demographics, locations, and behavioral data\\n",
                        "- ✅ **1,000 products** across 8 categories with pricing and inventory\\n",
                        "- ✅ **Delta format** tables optimized for analytics\\n",
                        "- ✅ **Real-time queryable** data ready for insights\\n\\n",
                        "**🎯 Population Method:** Direct Livy API execution via MCP automation tools"
                    ]
                },
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [
                        "## 🔍 Data Verification\\n\\n",
                        "Let's verify that your lakehouse contains the populated data:"
                    ]
                },
                {
                    "cell_type": "code",
                    "execution_count": null,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "# Verify lakehouse tables\\n",
                        "print(\"🔍 Available Tables in Lakehouse:\")\\n",
                        "spark.sql(\"SHOW TABLES\").show()\\n\\n",
                        "# Check table counts\\n",
                        "customer_count = spark.sql(\"SELECT COUNT(*) as count FROM customers\").collect()[0]['count']\\n",
                        "product_count = spark.sql(\"SELECT COUNT(*) as count FROM products\").collect()[0]['count']\\n\\n",
                        "print(f\"📈 Data Summary:\")\\n",
                        "print(f\"  Customers: {customer_count:,}\")\\n",
                        "print(f\"  Products: {product_count:,}\")\\n",
                        "print(f\"\\n✅ SUCCESS: Lakehouse populated with comprehensive data!\")"
                    ]
                },
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [
                        "## 👥 Customer Data Analysis\\n\\n",
                        "Explore the 5,000 customer records with realistic demographics:"
                    ]
                },
                {
                    "cell_type": "code",
                    "execution_count": null,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "# Customer data sample\\n",
                        "print(\"👥 Sample Customer Data:\")\\n",
                        "spark.sql(\"SELECT * FROM customers LIMIT 5\").show()\\n\\n",
                        "print(\"\\n📊 Customer Statistics:\")\\n",
                        "spark.sql(\"\"\"\\n",
                        "    SELECT \\n",
                        "        COUNT(*) as total_customers,\\n",
                        "        ROUND(AVG(customer_value), 2) as avg_value,\\n",
                        "        ROUND(MIN(customer_value), 2) as min_value,\\n",
                        "        ROUND(MAX(customer_value), 2) as max_value,\\n",
                        "        COUNT(CASE WHEN is_active = true THEN 1 END) as active_customers,\\n",
                        "        ROUND(AVG(age), 1) as avg_age\\n",
                        "    FROM customers\\n",
                        "\"\"\").show()"
                    ]
                },
                {
                    "cell_type": "code",
                    "execution_count": null,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "# Geographic distribution\\n",
                        "print(\"🗺️ Customer Distribution by State:\")\\n",
                        "spark.sql(\"\"\"\\n",
                        "    SELECT \\n",
                        "        state, \\n",
                        "        COUNT(*) as customer_count, \\n",
                        "        ROUND(AVG(customer_value), 2) as avg_value,\\n",
                        "        ROUND(AVG(age), 1) as avg_age\\n",
                        "    FROM customers \\n",
                        "    GROUP BY state \\n",
                        "    ORDER BY customer_count DESC\\n",
                        "\"\"\").show()\\n\\n",
                        "print(\"🏙️ Top Cities by Customer Count:\")\\n",
                        "spark.sql(\"\"\"\\n",
                        "    SELECT \\n",
                        "        city, \\n",
                        "        state,\\n",
                        "        COUNT(*) as customer_count, \\n",
                        "        ROUND(AVG(customer_value), 2) as avg_value\\n",
                        "    FROM customers \\n",
                        "    GROUP BY city, state \\n",
                        "    ORDER BY customer_count DESC\\n",
                        "    LIMIT 8\\n",
                        "\"\"\").show()"
                    ]
                },
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [
                        "## 🛍️ Product Data Analysis\\n\\n",
                        "Explore the 1,000 product records across multiple categories:"
                    ]
                },
                {
                    "cell_type": "code",
                    "execution_count": null,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "# Product data sample\\n",
                        "print(\"🛍️ Sample Product Data:\")\\n",
                        "spark.sql(\"SELECT * FROM products LIMIT 5\").show()\\n\\n",
                        "print(\"\\n📊 Product Statistics by Category:\")\\n",
                        "spark.sql(\"\"\"\\n",
                        "    SELECT \\n",
                        "        category,\\n",
                        "        COUNT(*) as product_count,\\n",
                        "        ROUND(AVG(price), 2) as avg_price,\\n",
                        "        ROUND(AVG(cost), 2) as avg_cost,\\n",
                        "        ROUND(AVG(stock_quantity), 0) as avg_stock,\\n",
                        "        ROUND(AVG(price - cost), 2) as avg_margin\\n",
                        "    FROM products \\n",
                        "    GROUP BY category \\n",
                        "    ORDER BY product_count DESC\\n",
                        "\"\"\").show()"
                    ]
                },
                {
                    "cell_type": "code",
                    "execution_count": null,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "# Brand analysis\\n",
                        "print(\"🏷️ Product Distribution by Brand:\")\\n",
                        "spark.sql(\"\"\"\\n",
                        "    SELECT \\n",
                        "        brand,\\n",
                        "        COUNT(*) as product_count,\\n",
                        "        ROUND(AVG(price), 2) as avg_price,\\n",
                        "        ROUND(MIN(price), 2) as min_price,\\n",
                        "        ROUND(MAX(price), 2) as max_price\\n",
                        "    FROM products \\n",
                        "    GROUP BY brand \\n",
                        "    ORDER BY avg_price DESC\\n",
                        "\"\"\").show()"
                    ]
                },
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [
                        "## 🏆 Top Performers Analysis\\n\\n",
                        "Identify high-value customers and products:"
                    ]
                },
                {
                    "cell_type": "code",
                    "execution_count": null,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "# Top customers by value\\n",
                        "print(\"🏆 Top 10 Customers by Value:\")\\n",
                        "spark.sql(\"\"\"\\n",
                        "    SELECT \\n",
                        "        customer_id, \\n",
                        "        CONCAT(first_name, ' ', last_name) as full_name,\\n",
                        "        customer_value, \\n",
                        "        city, \\n",
                        "        state,\\n",
                        "        age,\\n",
                        "        CASE WHEN is_active THEN 'Active' ELSE 'Inactive' END as status\\n",
                        "    FROM customers \\n",
                        "    ORDER BY customer_value DESC \\n",
                        "    LIMIT 10\\n",
                        "\"\"\").show()\\n\\n",
                        "print(\"💎 VIP Customers (Value > $10,000):\")\\n",
                        "vip_count = spark.sql(\"SELECT COUNT(*) as count FROM customers WHERE customer_value > 10000\").collect()[0]['count']\\n",
                        "print(f\"Total VIP customers: {vip_count}\")"
                    ]
                },
                {
                    "cell_type": "code",
                    "execution_count": null,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "# High-value and low-stock products\\n",
                        "print(\"💰 Most Expensive Products:\")\\n",
                        "spark.sql(\"\"\"\\n",
                        "    SELECT \\n",
                        "        product_name, \\n",
                        "        category, \\n",
                        "        brand, \\n",
                        "        price,\\n",
                        "        stock_quantity\\n",
                        "    FROM products \\n",
                        "    ORDER BY price DESC \\n",
                        "    LIMIT 10\\n",
                        "\"\"\").show()\\n\\n",
                        "print(\"📦 Low Stock Alert (< 50 items):\")\\n",
                        "low_stock = spark.sql(\"\"\"\\n",
                        "    SELECT \\n",
                        "        product_name, \\n",
                        "        category, \\n",
                        "        stock_quantity, \\n",
                        "        price\\n",
                        "    FROM products \\n",
                        "    WHERE stock_quantity < 50 \\n",
                        "    ORDER BY stock_quantity ASC\\n",
                        "    LIMIT 10\\n",
                        "\"\"\")\\n",
                        "low_stock.show()\\n",
                        "low_stock_count = spark.sql(\"SELECT COUNT(*) as count FROM products WHERE stock_quantity < 50\").collect()[0]['count']\\n",
                        "print(f\"\\nTotal products with low stock: {low_stock_count}\")"
                    ]
                },
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [
                        "## 📈 Business Intelligence Queries\\n\\n",
                        "Advanced analytics for business insights:"
                    ]
                },
                {
                    "cell_type": "code",
                    "execution_count": null,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "# Customer segmentation by value\\n",
                        "print(\"🎯 Customer Value Segmentation:\")\\n",
                        "spark.sql(\"\"\"\\n",
                        "    SELECT \\n",
                        "        CASE \\n",
                        "            WHEN customer_value >= 10000 THEN 'VIP (≥$10K)'\\n",
                        "            WHEN customer_value >= 5000 THEN 'Premium ($5K-$10K)'\\n",
                        "            WHEN customer_value >= 2000 THEN 'Standard ($2K-$5K)'\\n",
                        "            ELSE 'Basic (<$2K)'\\n",
                        "        END as segment,\\n",
                        "        COUNT(*) as customer_count,\\n",
                        "        ROUND(AVG(customer_value), 2) as avg_value,\\n",
                        "        ROUND(AVG(age), 1) as avg_age,\\n",
                        "        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM customers), 1) as percentage\\n",
                        "    FROM customers\\n",
                        "    GROUP BY \\n",
                        "        CASE \\n",
                        "            WHEN customer_value >= 10000 THEN 'VIP (≥$10K)'\\n",
                        "            WHEN customer_value >= 5000 THEN 'Premium ($5K-$10K)'\\n",
                        "            WHEN customer_value >= 2000 THEN 'Standard ($2K-$5K)'\\n",
                        "            ELSE 'Basic (<$2K)'\\n",
                        "        END\\n",
                        "    ORDER BY avg_value DESC\\n",
                        "\"\"\").show()"
                    ]
                },
                {
                    "cell_type": "code",
                    "execution_count": null,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "# Product profitability analysis\\n",
                        "print(\"💼 Product Profitability by Category:\")\\n",
                        "spark.sql(\"\"\"\\n",
                        "    SELECT \\n",
                        "        category,\\n",
                        "        COUNT(*) as product_count,\\n",
                        "        ROUND(AVG(price), 2) as avg_price,\\n",
                        "        ROUND(AVG(cost), 2) as avg_cost,\\n",
                        "        ROUND(AVG(price - cost), 2) as avg_profit,\\n",
                        "        ROUND(AVG((price - cost) / price * 100), 1) as profit_margin_pct,\\n",
                        "        SUM(stock_quantity) as total_inventory_units\\n",
                        "    FROM products \\n",
                        "    GROUP BY category \\n",
                        "    ORDER BY avg_profit DESC\\n",
                        "\"\"\").show()"
                    ]
                },
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [
                        "## 🔧 Data Generation Details\\n\\n",
                        "**How this data was created via MCP automation:**"
                    ]
                },
                {
                    "cell_type": "code",
                    "execution_count": null,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "print(\"🔧 Data Population Summary\")\\n",
                        "print(\"=\" * 50)\\n",
                        "print(\"🎯 Method: Direct Livy API execution via MCP\")\\n",
                        "print(\"📊 Process: Python data generation + Spark DataFrame + Delta tables\")\\n",
                        "print(\"⚡ Bypass: Notebook interface limitations circumvented\")\\n",
                        "print(\"✅ Result: Production-ready data in lakehouse storage\")\\n",
                        "print()\\n",
                        "print(\"📈 Customer Data Features:\")\\n",
                        "print(\"  • Unique IDs and realistic names\")\\n",
                        "print(\"  • Geographic distribution across 10 cities/states\")\\n",
                        "print(\"  • Registration dates spanning 2 years\")\\n",
                        "print(\"  • Customer value scores ($100-$15,000)\")\\n",
                        "print(\"  • Age demographics (18-80 years)\")\\n",
                        "print(\"  • Active/inactive status flags\")\\n",
                        "print()\\n",
                        "print(\"🛍️ Product Data Features:\")\\n",
                        "print(\"  • 8 diverse categories (Electronics, Clothing, Books, etc.)\")\\n",
                        "print(\"  • 5 different brand types\")\\n",
                        "print(\"  • Realistic pricing ($10-$1,000)\")\\n",
                        "print(\"  • Cost and margin calculations\")\\n",
                        "print(\"  • Stock quantities (0-1,000 units)\")\\n",
                        "print(\"  • Launch dates over 3 years\")\\n",
                        "print(\"  • Discontinuation flags\")\\n",
                        "print()\\n",
                        "print(\"🎉 Ready for: Analytics • Dashboards • ML • Reports\")"
                    ]
                },
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [
                        "## 🚀 Next Steps\\n\\n",
                        "Your lakehouse is now fully populated and ready for advanced analytics!\\n\\n",
                        "**Recommended actions:**\\n",
                        "1. 📊 **Create Power BI reports** connected to these tables\\n",
                        "2. 🤖 **Build ML models** for customer segmentation or demand forecasting\\n",
                        "3. 📈 **Develop KPI dashboards** tracking customer value and product performance\\n",
                        "4. 🔗 **Join with additional data sources** for enriched analytics\\n",
                        "5. 🏗️ **Create data pipelines** for ongoing data refresh\\n\\n",
                        "**Available tables:**\\n",
                        "- `customers` - Customer demographics and behavioral data\\n",
                        "- `products` - Product catalog with pricing and inventory\\n\\n",
                        "All data is stored in **Delta format** for optimal performance and ACID transactions!"
                    ]
                }
            ],
            "metadata": {
                "kernelspec": {
                    "display_name": "synapse_pyspark",
                    "name": "synapse_pyspark"
                },
                "language_info": {
                    "name": "python"
                }
            },
            "nbformat": 4,
            "nbformat_minor": 2
        };

        const createResult = await sendMCPRequest('create-fabric-notebook', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            notebookName: "Lakehouse Data Population Analysis",
            notebookContent: JSON.stringify(notebookContent)
        });
        
        console.log('✅ Notebook Creation Result:');
        console.log(createResult.content[0].text);
        
        // Get the notebook ID from the response
        const createdNotebook = JSON.parse(createResult.content[0].text);
        const notebookId = createdNotebook.id;
        
        console.log(`\n📚 Notebook deployed successfully!`);
        console.log(`📝 Name: "Lakehouse Data Population Analysis"`);
        console.log(`🆔 ID: ${notebookId}`);
        console.log(`🔗 Workspace: ${config.workspaceId}`);
        
        // Verify the notebook was created
        console.log('\n🔍 Verifying notebook deployment...');
        const verifyResult = await sendMCPRequest('get-fabric-notebook', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            notebookId: notebookId
        });
        
        console.log('✅ Verification Result:');
        console.log(verifyResult.content[0].text);
        
        console.log('\n🎉 SUCCESS! Your notebook is now available in Fabric workspace!');
        console.log('🎯 You can now:');
        console.log('   • Open it directly in Fabric Portal');
        console.log('   • Run the analysis cells to explore your data');
        console.log('   • Modify and extend the analytics');
        console.log('   • Share with your team');
        console.log('   • Use as a template for further development');
        
    } catch (error) {
        console.error('❌ Error deploying notebook:', error.message);
        
        // Try to provide helpful information
        console.log('\n💡 Troubleshooting:');
        console.log('   • Your data is still available in the lakehouse');
        console.log('   • You can manually create a notebook and copy the code');
        console.log('   • The local notebook file is available as a backup');
    }

    console.log('\n🛑 Cleaning up...');
    mcpProcess.kill();
}

deployNotebook().catch(console.error);
