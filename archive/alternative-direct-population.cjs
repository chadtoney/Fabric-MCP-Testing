const { spawn } = require('child_process');

const config = {
    workspaceId: "bcb44215-0e69-46d3-aac9-fb92fadcd982",
    testNotebookId: "5df3da0c-92c6-4e2d-82e8-d16bcb8572f3",
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

async function alternativeApproach() {
    console.log('🔄 Alternative Approach: Direct Data Population');
    console.log('='.repeat(50));
    console.log('💡 Since notebook content API has limitations,');
    console.log('   let\'s focus on populating data directly via Livy');
    console.log('='.repeat(50));
    
    const token = await getAzureToken();
    if (!token) {
        console.error('❌ Failed to get Azure token');
        return;
    }
    
    console.log(`✅ Got Azure token`);
    
    mcpProcess = spawn('node', ['C:\\Repos\\Fabric-Analytics-MCP\\build\\index.js'], {
        stdio: ['pipe', 'pipe', 'pipe'],
        cwd: 'C:\\Repos\\Fabric-Analytics-MCP'
    });

    await new Promise(resolve => setTimeout(resolve, 3000));

    try {
        console.log('\n🚀 Creating fresh Livy session for data population...');
        const sessionResult = await sendMCPRequest('create-livy-session', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            lakehouseId: config.lakehouseId,
            sessionConfig: {
                kind: 'pyspark',
                name: 'DirectDataPopulation'
            }
        });
        
        const sessionData = JSON.parse(sessionResult.content[0].text);
        const sessionId = sessionData.id;
        console.log(`✅ Created session: ${sessionId}`);
        
        console.log('⏳ Waiting 30 seconds for session to initialize...');
        await new Promise(resolve => setTimeout(resolve, 30000));
        
        // Execute a simple test first
        console.log('\n🧪 Testing session with simple code...');
        const testResult = await sendMCPRequest('execute-livy-statement', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            lakehouseId: config.lakehouseId,
            sessionId: sessionId,
            code: `
print("🚀 Session is ready!")
print(f"Spark version: {spark.version}")

# Test basic functionality
import pandas as pd
import numpy as np
from datetime import datetime

print("✅ Libraries imported successfully")
print(f"📅 Current time: {datetime.now()}")

# Show current tables
print("\\n📊 Current tables in lakehouse:")
spark.sql("SHOW TABLES").show()
            `,
            kind: 'pyspark'
        });
        
        console.log('\n📋 Test Result:');
        console.log('='.repeat(40));
        console.log(testResult.content[0].text);
        console.log('='.repeat(40));
        
        // Now execute the actual data population
        console.log('\n🚀 Executing comprehensive data population...');
        const dataPopulationCode = `
# Comprehensive data population for Fabric Lakehouse
print("🎯 Starting comprehensive data population...")

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Generate customer data
print("📊 Generating customer data...")

def generate_customer_data(num_customers=5000):
    first_names = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emily', 'Robert', 'Jessica', 'William', 'Ashley', 'Christopher', 'Amanda', 'Matthew', 'Melissa', 'Joshua']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson']
    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose']
    states = ['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX', 'CA', 'TX', 'CA']
    
    customers = []
    for i in range(num_customers):
        customer = {
            'customer_id': i + 1,
            'first_name': random.choice(first_names),
            'last_name': random.choice(last_names),
            'email': f"customer{i+1}@email.com",
            'city': random.choice(cities),
            'state': random.choice(states),
            'registration_date': datetime.now() - timedelta(days=random.randint(1, 730)),
            'is_active': random.choice([True, False]),
            'customer_value': round(random.uniform(100, 15000), 2),
            'age': random.randint(18, 80)
        }
        customers.append(customer)
    
    return pd.DataFrame(customers)

customers_df = generate_customer_data(5000)
print(f"✅ Generated {len(customers_df):,} customer records")

# Convert to Spark DataFrame and save
customers_spark_df = spark.createDataFrame(customers_df)
customers_spark_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("customers")
print("✅ Customers table created successfully")

# Generate product data
print("🛍️ Generating product data...")

def generate_product_data(num_products=1000):
    categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Toys', 'Health', 'Beauty']
    brands = ['Premium Brand', 'Value Brand', 'Luxury Brand', 'Eco Brand', 'Tech Brand']
    
    products = []
    for i in range(num_products):
        product = {
            'product_id': i + 1,
            'product_name': f"Product {i+1}",
            'category': random.choice(categories),
            'brand': random.choice(brands),
            'price': round(random.uniform(10, 1000), 2),
            'cost': round(random.uniform(5, 500), 2),
            'stock_quantity': random.randint(0, 1000),
            'launch_date': datetime.now() - timedelta(days=random.randint(30, 1095)),
            'is_discontinued': random.choice([True, False]) if random.random() < 0.1 else False
        }
        products.append(product)
    
    return pd.DataFrame(products)

products_df = generate_product_data(1000)
print(f"✅ Generated {len(products_df):,} product records")

# Convert to Spark DataFrame and save
products_spark_df = spark.createDataFrame(products_df)
products_spark_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("products")
print("✅ Products table created successfully")

print("🎉 Data population completed!")
print("\\n📊 Final verification:")
spark.sql("SHOW TABLES").show()

# Show counts
customer_count = spark.sql("SELECT COUNT(*) as count FROM customers").collect()[0]['count']
product_count = spark.sql("SELECT COUNT(*) as count FROM products").collect()[0]['count']

print(f"📈 Final counts:")
print(f"  Customers: {customer_count:,}")
print(f"  Products: {product_count:,}")

print("\\n🎉 SUCCESS! Lakehouse populated with comprehensive data!")
        `;
        
        const populationResult = await sendMCPRequest('execute-livy-statement', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            lakehouseId: config.lakehouseId,
            sessionId: sessionId,
            code: dataPopulationCode,
            kind: 'pyspark'
        });
        
        console.log('\n📋 Data Population Result:');
        console.log('='.repeat(50));
        console.log(populationResult.content[0].text);
        console.log('='.repeat(50));
        
        console.log('\n🎯 RECOMMENDATION FOR NOTEBOOK CONTENT:');
        console.log('Since API content access is limited, here\'s what you can do:');
        console.log('1. 📝 Manually copy/paste code into your Fabric notebook');
        console.log('2. ✅ Your data is now populated via direct execution');
        console.log('3. 🚀 You can run analytics queries on the populated tables');
        console.log('4. 📊 Build reports and dashboards on the data');
        
    } catch (error) {
        console.error('❌ Error in alternative approach:', error.message);
    }

    console.log('\n🛑 Cleaning up...');
    mcpProcess.kill();
}

alternativeApproach().catch(console.error);
