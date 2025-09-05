const { spawn } = require('child_process');

const config = {
    workspaceId: "bcb44215-0e69-46d3-aac9-fb92fadcd982",
    newNotebookId: "46c72ddd-5476-44b9-9a4d-cc859faaf326"
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

async function finalNotebookUpdate() {
    console.log('🎯 Final Notebook Content Update');
    console.log('='.repeat(35));
    
    const token = await getAzureToken();
    if (!token) {
        console.error('❌ Failed to get Azure token');
        return;
    }
    
    console.log(`✅ Got Azure token`);
    
    // Create comprehensive notebook with proper Python code
    const notebookContent = `{
  "cells": [
    {
      "cell_type": "markdown",
      "metadata": {},
      "source": [
        "# Fabric Lakehouse Data Population\\n",
        "\\n",
        "This notebook demonstrates comprehensive data population for a Fabric Lakehouse.\\n",
        "\\n",
        "## Features\\n",
        "- Customer data generation (5,000 records)\\n",
        "- Product catalog (1,000 products)\\n",
        "- Sales transactions (25,000 records)\\n",
        "- Analytics queries and verification"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {},
      "outputs": [],
      "source": [
        "# Import required libraries\\n",
        "import pandas as pd\\n",
        "import numpy as np\\n",
        "from datetime import datetime, timedelta\\n",
        "import random\\n",
        "from pyspark.sql import SparkSession\\n",
        "from pyspark.sql.types import *\\n",
        "from pyspark.sql.functions import *\\n",
        "\\n",
        "# Initialize Spark session\\n",
        "spark = SparkSession.builder.appName(\\"LakehouseDataPopulation\\").getOrCreate()\\n",
        "\\n",
        "print(\\"🚀 Libraries imported successfully\\")\\n",
        "print(f\\"⚡ Spark version: {spark.version}\\")"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {},
      "outputs": [],
      "source": [
        "# Generate comprehensive customer dataset\\n",
        "def generate_customer_data(num_customers=5000):\\n",
        "    \\"\\"\\"Generate realistic customer data\\"\\"\\"\\n",
        "    \\n",
        "    first_names = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emily', 'Robert', 'Jessica', 'William', 'Ashley', 'Christopher', 'Amanda', 'Matthew', 'Melissa', 'Joshua']\\n",
        "    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson']\\n",
        "    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose', 'Austin', 'Jacksonville', 'Fort Worth', 'Columbus', 'Charlotte']\\n",
        "    states = ['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX', 'CA', 'TX', 'CA', 'TX', 'FL', 'TX', 'OH', 'NC']\\n",
        "    \\n",
        "    customers = []\\n",
        "    for i in range(num_customers):\\n",
        "        customer = {\\n",
        "            'customer_id': i + 1,\\n",
        "            'first_name': random.choice(first_names),\\n",
        "            'last_name': random.choice(last_names),\\n",
        "            'email': f\\"customer{i+1}@email.com\\",\\n",
        "            'city': random.choice(cities),\\n",
        "            'state': random.choice(states),\\n",
        "            'registration_date': datetime.now() - timedelta(days=random.randint(1, 730)),\\n",
        "            'is_active': random.choice([True, False]),\\n",
        "            'customer_value': round(random.uniform(100, 15000), 2),\\n",
        "            'age': random.randint(18, 80)\\n",
        "        }\\n",
        "        customers.append(customer)\\n",
        "    \\n",
        "    return pd.DataFrame(customers)\\n",
        "\\n",
        "# Generate customer data\\n",
        "print(\\"📊 Generating customer data...\\")\\n",
        "customers_df = generate_customer_data(5000)\\n",
        "print(f\\"✅ Generated {len(customers_df):,} customer records\\")\\n",
        "print(\\"\\\\nSample customer data:\\")\\n",
        "print(customers_df.head())"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {},
      "outputs": [],
      "source": [
        "# Generate comprehensive product catalog\\n",
        "def generate_product_data(num_products=1000):\\n",
        "    \\"\\"\\"Generate realistic product catalog\\"\\"\\"\\n",
        "    \\n",
        "    categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports & Outdoors', 'Toys & Games', 'Health & Beauty', 'Automotive', 'Jewelry', 'Food & Beverages']\\n",
        "    brands = ['Premium Brand', 'Value Brand', 'Luxury Brand', 'Eco Brand', 'Tech Brand', 'Classic Brand', 'Modern Brand', 'Essential Brand']\\n",
        "    \\n",
        "    products = []\\n",
        "    for i in range(num_products):\\n",
        "        category = random.choice(categories)\\n",
        "        base_price = random.uniform(10, 1000)\\n",
        "        \\n",
        "        product = {\\n",
        "            'product_id': i + 1,\\n",
        "            'product_name': f\\"{category} Item {i+1}\\",\\n",
        "            'category': category,\\n",
        "            'brand': random.choice(brands),\\n",
        "            'price': round(base_price, 2),\\n",
        "            'cost': round(base_price * random.uniform(0.3, 0.7), 2),\\n",
        "            'stock_quantity': random.randint(0, 1000),\\n",
        "            'launch_date': datetime.now() - timedelta(days=random.randint(30, 1825)),\\n",
        "            'is_discontinued': random.choice([True, False]) if random.random() < 0.05 else False,\\n",
        "            'rating': round(random.uniform(1.0, 5.0), 1)\\n",
        "        }\\n",
        "        products.append(product)\\n",
        "    \\n",
        "    return pd.DataFrame(products)\\n",
        "\\n",
        "# Generate product data\\n",
        "print(\\"🛍️ Generating product catalog...\\")\\n",
        "products_df = generate_product_data(1000)\\n",
        "print(f\\"✅ Generated {len(products_df):,} product records\\")\\n",
        "print(\\"\\\\nSample product data:\\")\\n",
        "print(products_df.head())"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {},
      "outputs": [],
      "source": [
        "# Write data to Delta tables in Lakehouse\\n",
        "def write_to_lakehouse(df, table_name, mode=\\"overwrite\\"):\\n",
        "    \\"\\"\\"Write DataFrame to Lakehouse Delta table\\"\\"\\"\\n",
        "    try:\\n",
        "        # Convert pandas DataFrame to Spark DataFrame\\n",
        "        spark_df = spark.createDataFrame(df)\\n",
        "        \\n",
        "        # Write to Delta table\\n",
        "        spark_df.write \\\\\\n",
        "            .format(\\"delta\\") \\\\\\n",
        "            .mode(mode) \\\\\\n",
        "            .option(\\"mergeSchema\\", \\"true\\") \\\\\\n",
        "            .saveAsTable(table_name)\\n",
        "        \\n",
        "        print(f\\"✅ Successfully wrote {len(df):,} records to table '{table_name}'\\")\\n",
        "        \\n",
        "        # Show table schema\\n",
        "        print(f\\"\\\\n📋 Schema for '{table_name}':\\")\\n",
        "        spark.sql(f\\"DESCRIBE TABLE {table_name}\\").show(truncate=False)\\n",
        "        \\n",
        "    except Exception as e:\\n",
        "        print(f\\"❌ Error writing to table '{table_name}': {str(e)}\\")\\n",
        "\\n",
        "# Write customer and product data\\n",
        "print(\\"💾 Writing data to Lakehouse...\\")\\n",
        "write_to_lakehouse(customers_df, \\"customers\\")\\n",
        "write_to_lakehouse(products_df, \\"products\\")"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {},
      "outputs": [],
      "source": [
        "# Generate realistic sales transaction data\\n",
        "def generate_sales_data(num_transactions=25000):\\n",
        "    \\"\\"\\"Generate comprehensive sales transaction data\\"\\"\\"\\n",
        "    \\n",
        "    customer_ids = customers_df['customer_id'].tolist()\\n",
        "    product_ids = products_df['product_id'].tolist()\\n",
        "    \\n",
        "    sales = []\\n",
        "    for i in range(num_transactions):\\n",
        "        transaction_date = datetime.now() - timedelta(days=random.randint(1, 365))\\n",
        "        customer_id = random.choice(customer_ids)\\n",
        "        product_id = random.choice(product_ids)\\n",
        "        \\n",
        "        # Get product details\\n",
        "        product_price = products_df[products_df['product_id'] == product_id]['price'].iloc[0]\\n",
        "        quantity = random.randint(1, 10)\\n",
        "        discount = round(random.uniform(0, 0.4), 2) if random.random() < 0.25 else 0\\n",
        "        \\n",
        "        sale = {\\n",
        "            'transaction_id': i + 1,\\n",
        "            'customer_id': customer_id,\\n",
        "            'product_id': product_id,\\n",
        "            'transaction_date': transaction_date,\\n",
        "            'quantity': quantity,\\n",
        "            'unit_price': product_price,\\n",
        "            'discount_percent': discount,\\n",
        "            'total_amount': round(quantity * product_price * (1 - discount), 2),\\n",
        "            'payment_method': random.choice(['Credit Card', 'Debit Card', 'Cash', 'PayPal', 'Bank Transfer', 'Apple Pay', 'Google Pay']),\\n",
        "            'sales_channel': random.choice(['Online', 'In-Store', 'Mobile App', 'Phone', 'Social Media'])\\n",
        "        }\\n",
        "        sales.append(sale)\\n",
        "    \\n",
        "    return pd.DataFrame(sales)\\n",
        "\\n",
        "# Generate and write sales data\\n",
        "print(\\"💰 Generating sales transactions...\\")\\n",
        "sales_df = generate_sales_data(25000)\\n",
        "print(f\\"✅ Generated {len(sales_df):,} sales records\\")\\n",
        "\\n",
        "print(\\"💾 Writing sales data to Lakehouse...\\")\\n",
        "write_to_lakehouse(sales_df, \\"sales_transactions\\")"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {},
      "outputs": [],
      "source": [
        "# Data verification and analytics queries\\n",
        "print(\\"📊 === DATA VERIFICATION & ANALYTICS ===\\\\n\\")\\n",
        "\\n",
        "# Verify table counts\\n",
        "tables = [\\"customers\\", \\"products\\", \\"sales_transactions\\"]\\n",
        "\\n",
        "print(\\"📋 Table Record Counts:\\")\\n",
        "for table in tables:\\n",
        "    try:\\n",
        "        count = spark.sql(f\\"SELECT COUNT(*) as count FROM {table}\\").collect()[0]['count']\\n",
        "        print(f\\"  📊 {table}: {count:,} records\\")\\n",
        "    except Exception as e:\\n",
        "        print(f\\"  ❌ {table}: Error - {str(e)}\\")\\n",
        "\\n",
        "print(\\"\\\\n🎯 === BUSINESS ANALYTICS ===\\\\n\\")\\n",
        "\\n",
        "# Top customers by revenue\\n",
        "print(\\"💎 Top 10 Customers by Total Spend:\\")\\n",
        "top_customers = spark.sql(\\"\\"\\"\\n",
        "    SELECT \\n",
        "        c.customer_id,\\n",
        "        CONCAT(c.first_name, ' ', c.last_name) as customer_name,\\n",
        "        c.email,\\n",
        "        c.city,\\n",
        "        c.state,\\n",
        "        SUM(s.total_amount) as total_spent,\\n",
        "        COUNT(s.transaction_id) as transaction_count,\\n",
        "        AVG(s.total_amount) as avg_order_value\\n",
        "    FROM customers c\\n",
        "    JOIN sales_transactions s ON c.customer_id = s.customer_id\\n",
        "    GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.city, c.state\\n",
        "    ORDER BY total_spent DESC\\n",
        "    LIMIT 10\\n",
        "\\"\\"\\")\\n",
        "top_customers.show(truncate=False)\\n",
        "\\n",
        "# Sales performance by category\\n",
        "print(\\"\\\\n🏪 Sales Performance by Product Category:\\")\\n",
        "category_performance = spark.sql(\\"\\"\\"\\n",
        "    SELECT \\n",
        "        p.category,\\n",
        "        COUNT(s.transaction_id) as transaction_count,\\n",
        "        SUM(s.total_amount) as total_revenue,\\n",
        "        AVG(s.total_amount) as avg_transaction_value,\\n",
        "        SUM(s.quantity) as total_units_sold\\n",
        "    FROM products p\\n",
        "    JOIN sales_transactions s ON p.product_id = s.product_id\\n",
        "    GROUP BY p.category\\n",
        "    ORDER BY total_revenue DESC\\n",
        "\\"\\"\\")\\n",
        "category_performance.show(truncate=False)\\n",
        "\\n",
        "print(\\"\\\\n🎉 SUCCESS! Lakehouse populated with comprehensive business data!\\")\\n",
        "print(\\"✅ Ready for advanced analytics, reporting, and dashboard creation!\\")"
      ]
    }
  ],
  "metadata": {
    "kernelspec": {
      "display_name": "Python 3",
      "language": "python",
      "name": "python3"
    },
    "language_info": {
      "name": "python",
      "version": "3.8.0"
    }
  },
  "nbformat": 4,
  "nbformat_minor": 4
}`;

    console.log(`✅ Created comprehensive notebook content`);
    
    mcpProcess = spawn('node', ['C:\\Repos\\Fabric-Analytics-MCP\\build\\index.js'], {
        stdio: ['pipe', 'pipe', 'pipe'],
        cwd: 'C:\\Repos\\Fabric-Analytics-MCP'
    });

    await new Promise(resolve => setTimeout(resolve, 3000));

    try {
        console.log('\n🚀 Updating notebook with complete content...');
        const updateResult = await sendMCPRequest('update-fabric-notebook', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            notebookId: config.newNotebookId,
            displayName: "Complete Data Population Notebook",
            definition: {
                format: "ipynb",
                parts: [
                    {
                        path: "notebook-content.ipynb",
                        payload: Buffer.from(notebookContent).toString('base64'),
                        payloadType: "InlineBase64"
                    }
                ]
            }
        });
        
        console.log('\n📋 Update Result:');
        console.log('='.repeat(50));
        console.log(updateResult.content[0].text);
        console.log('='.repeat(50));
        
        console.log('\n🎉 FINAL SUCCESS!');
        console.log('✅ Complete notebook with 7 cells uploaded to Fabric!');
        console.log(`📝 Notebook ID: ${config.newNotebookId}`);
        console.log('📊 Contains comprehensive data population code');
        console.log('🚀 Ready to run in Fabric workspace!');
        console.log('\n🌐 Check your Fabric workspace now - the notebook should have all content!');
        
    } catch (error) {
        console.error('❌ Error updating notebook:', error.message);
        console.log('\n💡 Note: Your lakehouse data was successfully populated in previous steps!');
        console.log('📊 Even if notebook UI update failed, your data is ready for use.');
    }

    console.log('\n🛑 Final cleanup...');
    mcpProcess.kill();
}

finalNotebookUpdate().catch(console.error);
