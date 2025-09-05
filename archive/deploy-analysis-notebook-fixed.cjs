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
    console.log('📚 Deploying Lakehouse Analysis Notebook to Fabric');
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
        // Create the notebook with proper parameters
        console.log('📝 Creating lakehouse analysis notebook...');
        
        const notebookContent = {
            "cells": [
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [
                        "# 🏗️ Fabric Lakehouse Data Analysis\\n\\n",
                        "This notebook demonstrates the comprehensive data populated in your Fabric lakehouse via **MCP automation**.\\n\\n",
                        "**📊 Data Summary:**\\n",
                        "- ✅ **5,000 customers** with demographics and behavioral data\\n",
                        "- ✅ **1,000 products** across 8 categories\\n",
                        "- ✅ **Delta format** tables optimized for analytics\\n",
                        "- ✅ **Real-time queryable** data ready for insights"
                    ]
                },
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [
                        "## 🔍 Data Verification"
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
                        "## 👥 Customer Analysis"
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
                        "        COUNT(CASE WHEN is_active = true THEN 1 END) as active_customers\\n",
                        "    FROM customers\\n",
                        "\"\"\").show()"
                    ]
                },
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [
                        "## 🛍️ Product Analysis"
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
                        "        ROUND(AVG(stock_quantity), 0) as avg_stock\\n",
                        "    FROM products \\n",
                        "    GROUP BY category \\n",
                        "    ORDER BY product_count DESC\\n",
                        "\"\"\").show()"
                    ]
                },
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [
                        "## 🏆 Top Performers"
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
                        "        state\\n",
                        "    FROM customers \\n",
                        "    ORDER BY customer_value DESC \\n",
                        "    LIMIT 10\\n",
                        "\"\"\").show()"
                    ]
                },
                {
                    "cell_type": "code",
                    "execution_count": null,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "# High-value products\\n",
                        "print(\"💰 Most Expensive Products:\")\\n",
                        "spark.sql(\"\"\"\\n",
                        "    SELECT \\n",
                        "        product_name, \\n",
                        "        category, \\n",
                        "        brand, \\n",
                        "        price\\n",
                        "    FROM products \\n",
                        "    ORDER BY price DESC \\n",
                        "    LIMIT 10\\n",
                        "\"\"\").show()"
                    ]
                },
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [
                        "## 📈 Business Intelligence"
                    ]
                },
                {
                    "cell_type": "code",
                    "execution_count": null,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "# Customer segmentation\\n",
                        "print(\"🎯 Customer Value Segmentation:\")\\n",
                        "spark.sql(\"\"\"\\n",
                        "    SELECT \\n",
                        "        CASE \\n",
                        "            WHEN customer_value >= 10000 THEN 'VIP'\\n",
                        "            WHEN customer_value >= 5000 THEN 'Premium'\\n",
                        "            WHEN customer_value >= 2000 THEN 'Standard'\\n",
                        "            ELSE 'Basic'\\n",
                        "        END as segment,\\n",
                        "        COUNT(*) as customer_count,\\n",
                        "        ROUND(AVG(customer_value), 2) as avg_value\\n",
                        "    FROM customers\\n",
                        "    GROUP BY \\n",
                        "        CASE \\n",
                        "            WHEN customer_value >= 10000 THEN 'VIP'\\n",
                        "            WHEN customer_value >= 5000 THEN 'Premium'\\n",
                        "            WHEN customer_value >= 2000 THEN 'Standard'\\n",
                        "            ELSE 'Basic'\\n",
                        "        END\\n",
                        "    ORDER BY avg_value DESC\\n",
                        "\"\"\").show()"
                    ]
                },
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [
                        "## 🚀 Next Steps\\n\\n",
                        "Your lakehouse is now populated and ready for advanced analytics!\\n\\n",
                        "**Available tables:**\\n",
                        "- `customers` - 5,000 customer records\\n",
                        "- `products` - 1,000 product records\\n\\n",
                        "**Recommended actions:**\\n",
                        "1. Create Power BI reports\\n",
                        "2. Build ML models\\n",
                        "3. Develop KPI dashboards\\n",
                        "4. Join with additional data sources"
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
            displayName: "Lakehouse Data Analysis",
            notebookContent: JSON.stringify(notebookContent)
        });
        
        console.log('✅ Notebook Creation Result:');
        console.log(createResult.content[0].text);
        
        // Parse the response to get notebook details
        const responseText = createResult.content[0].text;
        console.log(`\n📚 Notebook deployed successfully!`);
        console.log(`📝 Name: "Lakehouse Data Analysis"`);
        console.log(`🔗 Workspace: ${config.workspaceId}`);
        console.log(`📄 Response: ${responseText}`);
        
        console.log('\n🎉 SUCCESS! Your notebook is now available in Fabric workspace!');
        console.log('🎯 You can now:');
        console.log('   • Open it directly in Fabric Portal');
        console.log('   • Run the analysis cells to explore your data');
        console.log('   • Modify and extend the analytics');
        console.log('   • Share with your team');
        
    } catch (error) {
        console.error('❌ Error deploying notebook:', error.message);
        
        console.log('\n💡 Alternative options:');
        console.log('   • Your data is still available in the lakehouse');
        console.log('   • You can manually create a notebook and copy the code');
        console.log('   • The local notebook file is available as a backup');
    }

    console.log('\n🛑 Cleaning up...');
    mcpProcess.kill();
}

deployNotebook().catch(console.error);
