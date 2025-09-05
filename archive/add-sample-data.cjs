const { spawn } = require('child_process');

// Configuration
const config = {
  workspaceId: process.env.FABRIC_DEFAULT_WORKSPACE_ID || 'bcb44215-0e69-46d3-aac9-fb92fadcd982',
  bearerToken: 'azure_cli',
  lakehouseId: '5e6b33fe-1f33-419a-a954-bce697ccfe61' // ID from the created lakehouse
};

console.log('📊 Adding Sample Data to Microsoft Fabric Lakehouse via MCP');
console.log(`Workspace: ${config.workspaceId}`);
console.log(`Lakehouse: ${config.lakehouseId}`);

// Start the MCP server process
const mcpServer = spawn('node', ['build/index.js'], {
  stdio: ['pipe', 'pipe', 'pipe'],
  env: {
    ...process.env,
    FABRIC_AUTH_METHOD: 'azure_cli',
    FABRIC_DEFAULT_WORKSPACE_ID: config.workspaceId
  }
});

let requestId = 1;
let responseBuffer = '';

function sendMcpRequest(method, params = {}) {
  const request = {
    jsonrpc: '2.0',
    id: requestId++,
    method: method,
    params: params
  };
  
  console.log(`📤 Sending MCP request: ${method}`);
  mcpServer.stdin.write(JSON.stringify(request) + '\n');
  
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error(`Request ${method} timed out after 60 seconds`));
    }, 60000); // Increased timeout for Spark operations
    
    function processResponse(data) {
      responseBuffer += data.toString();
      const lines = responseBuffer.split('\n');
      
      for (let i = 0; i < lines.length - 1; i++) {
        const line = lines[i].trim();
        if (line) {
          try {
            const response = JSON.parse(line);
            if (response.id === request.id) {
              clearTimeout(timeout);
              mcpServer.stdout.removeListener('data', processResponse);
              responseBuffer = lines[lines.length - 1];
              resolve(response);
              return;
            }
          } catch (error) {
            // Ignore JSON parse errors for incomplete lines
          }
        }
      }
      responseBuffer = lines[lines.length - 1];
    }
    
    mcpServer.stdout.on('data', processResponse);
  });
}

// Sample data generation code
const sampleDataCode = `
# Create sample sales data
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

print("🚀 Starting sample data generation...")

# Create sample sales data
sales_data = []
products = ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones', 'Tablet', 'Phone', 'Camera']
regions = ['North', 'South', 'East', 'West', 'Central']
sales_reps = ['Alice Johnson', 'Bob Smith', 'Carol Davis', 'David Wilson', 'Eva Brown']

for i in range(1000):
    date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))
    sales_data.append({
        'sale_id': i + 1,
        'product': random.choice(products),
        'quantity': random.randint(1, 10),
        'unit_price': round(random.uniform(50, 2000), 2),
        'region': random.choice(regions),
        'sales_rep': random.choice(sales_reps),
        'sale_date': date.strftime('%Y-%m-%d')
    })

# Create DataFrame
sales_df = spark.createDataFrame(sales_data)
sales_df = sales_df.withColumn('total_amount', col('quantity') * col('unit_price'))

print(f"✅ Created sales dataset with {sales_df.count()} records")
sales_df.show(10)

# Save to lakehouse
sales_df.write.mode('overwrite').saveAsTable('sales_data')
print("✅ Saved sales_data table to lakehouse")
`;

const customerDataCode = `
# Create sample customer data
customer_data = []
first_names = ['John', 'Jane', 'Mike', 'Sarah', 'Tom', 'Lisa', 'David', 'Emma', 'Chris', 'Anna']
last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez']
cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose']

for i in range(500):
    customer_data.append({
        'customer_id': i + 1,
        'first_name': random.choice(first_names),
        'last_name': random.choice(last_names),
        'email': f'customer{i+1}@email.com',
        'city': random.choice(cities),
        'age': random.randint(18, 80),
        'signup_date': (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 730))).strftime('%Y-%m-%d')
    })

# Create DataFrame
customers_df = spark.createDataFrame(customer_data)

print(f"✅ Created customer dataset with {customers_df.count()} records")
customers_df.show(10)

# Save to lakehouse
customers_df.write.mode('overwrite').saveAsTable('customers')
print("✅ Saved customers table to lakehouse")
`;

const summaryCode = `
# Create summary analytics
print("📊 Creating summary analytics...")

# Sales summary by product
product_summary = spark.sql("""
    SELECT 
        product,
        COUNT(*) as total_sales,
        SUM(quantity) as total_quantity,
        SUM(total_amount) as total_revenue,
        AVG(total_amount) as avg_sale_amount
    FROM sales_data
    GROUP BY product
    ORDER BY total_revenue DESC
""")

print("🎯 Sales Summary by Product:")
product_summary.show()

# Sales by region
region_summary = spark.sql("""
    SELECT 
        region,
        COUNT(*) as total_sales,
        SUM(total_amount) as total_revenue,
        AVG(total_amount) as avg_sale_amount
    FROM sales_data
    GROUP BY region
    ORDER BY total_revenue DESC
""")

print("🗺️ Sales Summary by Region:")
region_summary.show()

# Save summaries
product_summary.write.mode('overwrite').saveAsTable('product_summary')
region_summary.write.mode('overwrite').saveAsTable('region_summary')

print("✅ All sample data and analytics tables created successfully!")
print("📋 Tables created in lakehouse:")
print("  - sales_data (1000 records)")
print("  - customers (500 records)")
print("  - product_summary (analytics)")
print("  - region_summary (analytics)")
`;

async function addSampleData() {
  try {
    console.log('\n🔄 Step 1: Creating Livy session...');
    const sessionResponse = await sendMcpRequest('tools/call', {
      name: 'create-livy-session',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        lakehouseId: config.lakehouseId,
        sessionConfig: {
          kind: 'pyspark',
          name: 'sample-data-session',
          driverMemory: '4g',
          executorMemory: '2g',
          numExecutors: 2
        }
      }
    });
    
    console.log('✅ Session creation response:');
    console.log(JSON.stringify(sessionResponse, null, 2));
    
    if (sessionResponse.result && sessionResponse.result.content) {
      const content = sessionResponse.result.content[0].text;
      const sessionData = JSON.parse(content);
      const sessionId = sessionData.id;
      
      console.log(`🎯 Created Livy session with ID: ${sessionId}`);
      
      // Wait for session to be ready
      console.log('\n⏳ Waiting for session to start...');
      await new Promise(resolve => setTimeout(resolve, 10000));
      
      console.log('\n📊 Step 2: Executing sales data generation...');
      const salesResponse = await sendMcpRequest('tools/call', {
        name: 'execute-livy-statement',
        arguments: {
          bearerToken: config.bearerToken,
          workspaceId: config.workspaceId,
          lakehouseId: config.lakehouseId,
          sessionId: sessionId,
          code: sampleDataCode,
          kind: 'pyspark'
        }
      });
      
      console.log('✅ Sales data execution response:');
      console.log(JSON.stringify(salesResponse, null, 2));
      
      // Wait for execution
      await new Promise(resolve => setTimeout(resolve, 15000));
      
      console.log('\n👥 Step 3: Executing customer data generation...');
      const customerResponse = await sendMcpRequest('tools/call', {
        name: 'execute-livy-statement',
        arguments: {
          bearerToken: config.bearerToken,
          workspaceId: config.workspaceId,
          lakehouseId: config.lakehouseId,
          sessionId: sessionId,
          code: customerDataCode,
          kind: 'pyspark'
        }
      });
      
      console.log('✅ Customer data execution response:');
      console.log(JSON.stringify(customerResponse, null, 2));
      
      // Wait for execution
      await new Promise(resolve => setTimeout(resolve, 15000));
      
      console.log('\n📈 Step 4: Creating summary analytics...');
      const summaryResponse = await sendMcpRequest('tools/call', {
        name: 'execute-livy-statement',
        arguments: {
          bearerToken: config.bearerToken,
          workspaceId: config.workspaceId,
          lakehouseId: config.lakehouseId,
          sessionId: sessionId,
          code: summaryCode,
          kind: 'pyspark'
        }
      });
      
      console.log('✅ Summary analytics execution response:');
      console.log(JSON.stringify(summaryResponse, null, 2));
      
      console.log('\n🎉 Sample data generation completed!');
      console.log(`✅ Lakehouse: MCPDemoLakehouse (${config.lakehouseId})`);
      console.log('📋 Tables created:');
      console.log('  - sales_data (1000 sales records)');
      console.log('  - customers (500 customer records)');
      console.log('  - product_summary (analytics by product)');
      console.log('  - region_summary (analytics by region)');
      console.log(`🌐 View in portal: https://msit.powerbi.com/groups/${config.workspaceId}`);
    }
    
  } catch (error) {
    console.error('❌ Error adding sample data:', error.message);
    console.error('Stack:', error.stack);
  } finally {
    console.log('\n🛑 Stopping MCP server...');
    mcpServer.kill();
    setTimeout(() => process.exit(0), 2000);
  }
}

// Handle MCP server startup
mcpServer.stderr.on('data', (data) => {
  const message = data.toString();
  console.log('🔧 MCP Server:', message.trim());
  
  if (message.includes('Microsoft Fabric Analytics MCP Server running')) {
    console.log('✅ MCP Server started successfully');
    setTimeout(addSampleData, 3000);
  }
});

mcpServer.stdout.on('data', (data) => {
  const message = data.toString();
  if (!message.trim().startsWith('{')) {
    console.log('📤 MCP Output:', message.trim());
  }
});

mcpServer.on('error', (error) => {
  console.error('❌ MCP Server error:', error);
  process.exit(1);
});

mcpServer.on('exit', (code) => {
  console.log(`MCP Server exited with code ${code}`);
});
