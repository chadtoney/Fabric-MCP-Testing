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

async function createCompleteNotebook() {
    console.log('🚀 Creating Complete Jupyter Notebook');
    console.log('='.repeat(40));
    
    const token = await getAzureToken();
    if (!token) {
        console.error('❌ Failed to get Azure token');
        return;
    }
    
    console.log(`✅ Got Azure token`);
    
    // Create a proper Jupyter notebook structure with all our content
    const jupyterNotebook = {
        cells: [
            {
                cell_type: "markdown",
                metadata: {},
                source: [
                    "# Fabric Lakehouse Data Population\n",
                    "\n",
                    "This notebook demonstrates how to populate a Fabric Lakehouse with sample data for testing and development purposes.\n",
                    "\n",
                    "## Overview\n",
                    "- Create sample datasets\n",
                    "- Write data to lakehouse tables\n",
                    "- Verify data ingestion\n",
                    "- Set up basic data structures for analytics"
                ]
            },
            {
                cell_type: "code",
                execution_count: null,
                metadata: {},
                outputs: [],
                source: [
                    "# Import required libraries\n",
                    "import pandas as pd\n",
                    "import numpy as np\n",
                    "from datetime import datetime, timedelta\n",
                    "import random\n",
                    "from pyspark.sql import SparkSession\n",
                    "from pyspark.sql.types import *\n",
                    "from pyspark.sql.functions import *\n",
                    "\n",
                    "# Initialize Spark session\n",
                    "spark = SparkSession.builder.appName(\"LakehouseDataPopulation\").getOrCreate()\n",
                    "\n",
                    "print(\"Libraries imported successfully\")\n",
                    "print(f\"Spark version: {spark.version}\")"
                ]
            },
            {
                cell_type: "code",
                execution_count: null,
                metadata: {},
                outputs: [],
                source: [
                    "# Create sample customer data\n",
                    "def generate_customer_data(num_customers=10000):\n",
                    "    \"\"\"Generate sample customer data\"\"\"\n",
                    "    \n",
                    "    # Sample data for names and locations\n",
                    "    first_names = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emily', 'Robert', 'Jessica', 'William', 'Ashley']\n",
                    "    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez']\n",
                    "    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose']\n",
                    "    states = ['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX', 'CA', 'TX', 'CA']\n",
                    "    \n",
                    "    customers = []\n",
                    "    for i in range(num_customers):\n",
                    "        customer = {\n",
                    "            'customer_id': i + 1,\n",
                    "            'first_name': random.choice(first_names),\n",
                    "            'last_name': random.choice(last_names),\n",
                    "            'email': f\"user{i+1}@example.com\",\n",
                    "            'city': random.choice(cities),\n",
                    "            'state': random.choice(states),\n",
                    "            'registration_date': datetime.now() - timedelta(days=random.randint(1, 365)),\n",
                    "            'is_active': random.choice([True, False]),\n",
                    "            'customer_value': round(random.uniform(100, 10000), 2)\n",
                    "        }\n",
                    "        customers.append(customer)\n",
                    "    \n",
                    "    return pd.DataFrame(customers)\n",
                    "\n",
                    "# Generate customer data\n",
                    "customers_df = generate_customer_data(5000)\n",
                    "print(f\"Generated {len(customers_df)} customer records\")\n",
                    "print(\"\\nSample data:\")\n",
                    "print(customers_df.head())"
                ]
            },
            {
                cell_type: "code",
                execution_count: null,
                metadata: {},
                outputs: [],
                source: [
                    "# Create sample product data\n",
                    "def generate_product_data(num_products=1000):\n",
                    "    \"\"\"Generate sample product data\"\"\"\n",
                    "    \n",
                    "    categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Toys', 'Health', 'Beauty']\n",
                    "    brands = ['Brand A', 'Brand B', 'Brand C', 'Brand D', 'Brand E']\n",
                    "    \n",
                    "    products = []\n",
                    "    for i in range(num_products):\n",
                    "        product = {\n",
                    "            'product_id': i + 1,\n",
                    "            'product_name': f\"Product {i+1}\",\n",
                    "            'category': random.choice(categories),\n",
                    "            'brand': random.choice(brands),\n",
                    "            'price': round(random.uniform(10, 500), 2),\n",
                    "            'cost': round(random.uniform(5, 300), 2),\n",
                    "            'stock_quantity': random.randint(0, 1000),\n",
                    "            'launch_date': datetime.now() - timedelta(days=random.randint(30, 1095)),\n",
                    "            'is_discontinued': random.choice([True, False]) if random.random() < 0.1 else False\n",
                    "        }\n",
                    "        products.append(product)\n",
                    "    \n",
                    "    return pd.DataFrame(products)\n",
                    "\n",
                    "# Generate product data\n",
                    "products_df = generate_product_data(1000)\n",
                    "print(f\"Generated {len(products_df)} product records\")\n",
                    "print(\"\\nSample data:\")\n",
                    "print(products_df.head())"
                ]
            },
            {
                cell_type: "code",
                execution_count: null,
                metadata: {},
                outputs: [],
                source: [
                    "# Write data to Lakehouse tables\n",
                    "def write_to_lakehouse(df, table_name, mode=\"overwrite\"):\n",
                    "    \"\"\"Write DataFrame to Lakehouse table\"\"\"\n",
                    "    try:\n",
                    "        # Convert pandas DataFrame to Spark DataFrame\n",
                    "        spark_df = spark.createDataFrame(df)\n",
                    "        \n",
                    "        # Write to Delta table in the lakehouse\n",
                    "        spark_df.write \\\n",
                    "            .format(\"delta\") \\\n",
                    "            .mode(mode) \\\n",
                    "            .option(\"mergeSchema\", \"true\") \\\n",
                    "            .saveAsTable(table_name)\n",
                    "        \n",
                    "        print(f\"Successfully wrote {len(df)} records to table '{table_name}'\")\n",
                    "        \n",
                    "        # Show table info\n",
                    "        table_info = spark.sql(f\"DESCRIBE TABLE {table_name}\")\n",
                    "        print(f\"\\nTable schema for '{table_name}':\")\n",
                    "        table_info.show()\n",
                    "        \n",
                    "    except Exception as e:\n",
                    "        print(f\"Error writing to table '{table_name}': {str(e)}\")\n",
                    "\n",
                    "# Write all datasets to lakehouse\n",
                    "print(\"Writing customer data to lakehouse...\")\n",
                    "write_to_lakehouse(customers_df, \"customers\")\n",
                    "\n",
                    "print(\"\\nWriting product data to lakehouse...\")\n",
                    "write_to_lakehouse(products_df, \"products\")"
                ]
            },
            {
                cell_type: "code",
                execution_count: null,
                metadata: {},
                outputs: [],
                source: [
                    "# Create sample sales transaction data\n",
                    "def generate_sales_data(num_transactions=50000, customers_df=None, products_df=None):\n",
                    "    \"\"\"Generate sample sales transaction data\"\"\"\n",
                    "    \n",
                    "    if customers_df is None or products_df is None:\n",
                    "        raise ValueError(\"Customer and product data required\")\n",
                    "    \n",
                    "    customer_ids = customers_df['customer_id'].tolist()\n",
                    "    product_ids = products_df['product_id'].tolist()\n",
                    "    \n",
                    "    sales = []\n",
                    "    for i in range(num_transactions):\n",
                    "        transaction_date = datetime.now() - timedelta(days=random.randint(1, 365))\n",
                    "        customer_id = random.choice(customer_ids)\n",
                    "        product_id = random.choice(product_ids)\n",
                    "        \n",
                    "        # Get product price for this transaction\n",
                    "        product_price = products_df[products_df['product_id'] == product_id]['price'].iloc[0]\n",
                    "        quantity = random.randint(1, 5)\n",
                    "        discount = round(random.uniform(0, 0.3), 2) if random.random() < 0.2 else 0\n",
                    "        \n",
                    "        sale = {\n",
                    "            'transaction_id': i + 1,\n",
                    "            'customer_id': customer_id,\n",
                    "            'product_id': product_id,\n",
                    "            'transaction_date': transaction_date,\n",
                    "            'quantity': quantity,\n",
                    "            'unit_price': product_price,\n",
                    "            'discount_percent': discount,\n",
                    "            'total_amount': round(quantity * product_price * (1 - discount), 2),\n",
                    "            'payment_method': random.choice(['Credit Card', 'Debit Card', 'Cash', 'PayPal', 'Bank Transfer']),\n",
                    "            'sales_channel': random.choice(['Online', 'In-Store', 'Mobile App', 'Phone'])\n",
                    "        }\n",
                    "        sales.append(sale)\n",
                    "    \n",
                    "    return pd.DataFrame(sales)\n",
                    "\n",
                    "# Generate sales data\n",
                    "sales_df = generate_sales_data(25000, customers_df, products_df)\n",
                    "print(f\"Generated {len(sales_df)} sales transaction records\")\n",
                    "print(\"\\nSample data:\")\n",
                    "print(sales_df.head())\n",
                    "\n",
                    "# Write sales data to lakehouse\n",
                    "print(\"\\nWriting sales data to lakehouse...\")\n",
                    "write_to_lakehouse(sales_df, \"sales_transactions\")"
                ]
            },
            {
                cell_type: "code",
                execution_count: null,
                metadata: {},
                outputs: [],
                source: [
                    "# Verify data ingestion and run sample queries\n",
                    "print(\"=== DATA VERIFICATION ===\\n\")\n",
                    "\n",
                    "# Check table counts\n",
                    "tables = [\"customers\", \"products\", \"sales_transactions\"]\n",
                    "\n",
                    "for table in tables:\n",
                    "    try:\n",
                    "        count = spark.sql(f\"SELECT COUNT(*) as count FROM {table}\").collect()[0]['count']\n",
                    "        print(f\"Table '{table}': {count:,} records\")\n",
                    "    except Exception as e:\n",
                    "        print(f\"Error querying table '{table}': {str(e)}\")\n",
                    "\n",
                    "print(\"\\n=== SAMPLE QUERIES ===\\n\")\n",
                    "\n",
                    "# Sample query 1: Top customers by total spend\n",
                    "print(\"Top 10 customers by total spend:\")\n",
                    "top_customers = spark.sql(\"\"\"\n",
                    "    SELECT \n",
                    "        c.customer_id,\n",
                    "        c.first_name,\n",
                    "        c.last_name,\n",
                    "        c.email,\n",
                    "        SUM(s.total_amount) as total_spent,\n",
                    "        COUNT(s.transaction_id) as transaction_count\n",
                    "    FROM customers c\n",
                    "    JOIN sales_transactions s ON c.customer_id = s.customer_id\n",
                    "    GROUP BY c.customer_id, c.first_name, c.last_name, c.email\n",
                    "    ORDER BY total_spent DESC\n",
                    "    LIMIT 10\n",
                    "\"\"\")\n",
                    "top_customers.show()\n",
                    "\n",
                    "# Sample query 2: Sales by category\n",
                    "print(\"\\nSales by product category:\")\n",
                    "sales_by_category = spark.sql(\"\"\"\n",
                    "    SELECT \n",
                    "        p.category,\n",
                    "        COUNT(s.transaction_id) as transaction_count,\n",
                    "        SUM(s.total_amount) as total_revenue,\n",
                    "        AVG(s.total_amount) as avg_transaction_value\n",
                    "    FROM products p\n",
                    "    JOIN sales_transactions s ON p.product_id = s.product_id\n",
                    "    GROUP BY p.category\n",
                    "    ORDER BY total_revenue DESC\n",
                    "\"\"\")\n",
                    "sales_by_category.show()"
                ]
            },
            {
                cell_type: "markdown",
                metadata: {},
                source: [
                    "## Summary\n",
                    "\n",
                    "This notebook has successfully:\n",
                    "- Generated comprehensive sample datasets\n",
                    "- Created customers, products, and sales transaction tables\n",
                    "- Populated the Fabric Lakehouse with realistic data\n",
                    "- Demonstrated basic analytics queries\n",
                    "\n",
                    "The lakehouse is now ready for advanced analytics, reporting, and dashboard creation!"
                ]
            }
        ],
        metadata: {
            kernelspec: {
                display_name: "Python 3",
                language: "python",
                name: "python3"
            },
            language_info: {
                name: "python",
                version: "3.8.0"
            }
        },
        nbformat: 4,
        nbformat_minor: 4
    };
    
    console.log(`✅ Created Jupyter notebook with ${jupyterNotebook.cells.length} cells`);
    
    const jupyterContent = JSON.stringify(jupyterNotebook, null, 2);
    const base64Content = Buffer.from(jupyterContent).toString('base64');
    
    mcpProcess = spawn('node', ['C:\\Repos\\Fabric-Analytics-MCP\\build\\index.js'], {
        stdio: ['pipe', 'pipe', 'pipe'],
        cwd: 'C:\\Repos\\Fabric-Analytics-MCP'
    });

    await new Promise(resolve => setTimeout(resolve, 3000));

    try {
        console.log('\n🚀 Updating notebook with complete content...');
        const updateResult = await sendMCPRequest('update-fabric-notebook-definition', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            notebookId: config.newNotebookId,
            definition: {
                format: "ipynb",
                parts: [
                    {
                        path: "notebook-content.ipynb",
                        payload: base64Content,
                        payloadType: "InlineBase64"
                    }
                ]
            }
        });
        
        console.log('\n📋 Update Result:');
        console.log('='.repeat(40));
        console.log(updateResult.content[0].text);
        console.log('='.repeat(40));
        
        console.log('\n🎉 SUCCESS! Complete notebook uploaded to Fabric!');
        console.log('✅ Notebook contains 8 cells with full data population code');
        console.log(`📝 Notebook ID: ${config.newNotebookId}`);
        console.log('📊 Ready to run in Fabric to populate your lakehouse');
        console.log('🌐 Check your Fabric workspace - the notebook should now have all content!');
        
    } catch (error) {
        console.error('❌ Error updating notebook:', error.message);
    }

    console.log('\n🛑 Cleaning up...');
    mcpProcess.kill();
}

createCompleteNotebook().catch(console.error);
