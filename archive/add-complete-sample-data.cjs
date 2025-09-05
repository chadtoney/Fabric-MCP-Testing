const { spawn } = require('child_process');

// Configuration
const config = {
  workspaceId: process.env.FABRIC_DEFAULT_WORKSPACE_ID || 'bcb44215-0e69-46d3-aac9-fb92fadcd982',
  bearerToken: 'azure_cli',
  lakehouseId: '5e6b33fe-1f33-419a-a954-bce697ccfe61' // ID from the created lakehouse
};

console.log('📊 Adding Sample Data to Microsoft Fabric Lakehouse via MCP (Complete)');
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
      reject(new Error(`Request ${method} timed out after 90 seconds`));
    }, 90000);
    
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

// Enhanced sample data generation code
const sampleDataCode = `
print("🚀 Starting comprehensive sample data generation for Fabric Lakehouse...")

# Import required libraries
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

# Clear any existing data first
print("🧹 Cleaning up any existing demo tables...")
try:
    spark.sql("DROP TABLE IF EXISTS demo_sales")
    spark.sql("DROP TABLE IF EXISTS demo_customers") 
    spark.sql("DROP TABLE IF EXISTS sales_summary")
    print("✅ Cleaned up existing tables")
except:
    print("ℹ️ No existing tables to clean")

# 1. Create comprehensive sales data
print("📊 Creating sales data...")
sales_data = []
products = ['Laptop Pro', 'Wireless Mouse', 'Mechanical Keyboard', '4K Monitor', 'Gaming Headphones', 'Tablet', 'Smartphone', 'Smart Watch']
regions = ['North America', 'Europe', 'Asia Pacific', 'South America']
sales_reps = ['Alice Johnson', 'Bob Smith', 'Carol Williams', 'David Brown', 'Emma Davis']

# Generate 500 sales records
for i in range(500):
    order_date = datetime.now() - timedelta(days=random.randint(1, 365))
    product = products[i % len(products)]
    quantity = random.randint(1, 10)
    base_price = {
        'Laptop Pro': 1299.99,
        'Wireless Mouse': 79.99,
        'Mechanical Keyboard': 149.99,
        '4K Monitor': 899.99,
        'Gaming Headphones': 199.99,
        'Tablet': 599.99,
        'Smartphone': 899.99,
        'Smart Watch': 399.99
    }[product]
    
    unit_price = round(base_price * (0.8 + random.random() * 0.4), 2)  # ±20% price variation
    total_amount = round(quantity * unit_price, 2)
    
    sales_data.append(Row(
        sale_id=f"ORD-{1000 + i}",
        order_date=order_date.strftime("%Y-%m-%d"),
        product_name=product,
        category=("Electronics" if product in ['Laptop Pro', 'Tablet', 'Smartphone'] 
                 else "Accessories" if product in ['Wireless Mouse', 'Mechanical Keyboard', 'Gaming Headphones']
                 else "Displays" if product == '4K Monitor' else "Wearables"),
        quantity=quantity,
        unit_price=unit_price,
        total_amount=total_amount,
        region=regions[i % len(regions)],
        sales_rep=sales_reps[i % len(sales_reps)],
        customer_id=f"CUST-{random.randint(1001, 1500)}"
    ))

# Create sales DataFrame
sales_df = spark.createDataFrame(sales_data)
print(f"✅ Created sales dataset with {sales_df.count()} records")

# Show sample data
print("📋 Sample sales data:")
sales_df.show(10, truncate=False)

# Save sales data to lakehouse
print("💾 Saving sales data to lakehouse...")
sales_df.write.format("delta").mode('overwrite').option("mergeSchema", "true").saveAsTable('demo_sales')
print("✅ Saved demo_sales table to lakehouse")

# 2. Create customer data
print("👥 Creating customer data...")
customer_data = []
companies = ['TechCorp Inc', 'Global Solutions', 'Innovation Labs', 'Digital Dynamics', 'Future Systems', 'Smart Enterprises']
titles = ['CEO', 'CTO', 'IT Manager', 'Developer', 'Business Analyst', 'Project Manager']

for i in range(100):
    customer_data.append(Row(
        customer_id=f"CUST-{1001 + i}",
        customer_name=f"Customer {i+1}",
        company=companies[i % len(companies)],
        title=titles[i % len(titles)],
        email=f"customer{i+1}@{companies[i % len(companies)].lower().replace(' ', '').replace('inc', 'com')}",
        phone=f"+1-555-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
        region=regions[i % len(regions)],
        created_date=(datetime.now() - timedelta(days=random.randint(30, 730))).strftime("%Y-%m-%d")
    ))

# Create customers DataFrame
customers_df = spark.createDataFrame(customer_data)
print(f"✅ Created customers dataset with {customers_df.count()} records")

# Show sample customer data
print("📋 Sample customer data:")
customers_df.show(5, truncate=False)

# Save customer data
print("💾 Saving customer data to lakehouse...")
customers_df.write.format("delta").mode('overwrite').option("mergeSchema", "true").saveAsTable('demo_customers')
print("✅ Saved demo_customers table to lakehouse")

# 3. Create comprehensive analytics summary
print("📈 Creating sales analytics summary...")

# Sales by product
product_summary = sales_df.groupBy("product_name", "category").agg(
    count("*").alias("total_orders"),
    sum("quantity").alias("total_units_sold"),
    sum("total_amount").alias("total_revenue"),
    avg("total_amount").alias("avg_order_value"),
    min("order_date").alias("first_sale"),
    max("order_date").alias("last_sale")
).orderBy(desc("total_revenue"))

print("📊 Product Performance Summary:")
product_summary.show(truncate=False)

# Regional performance
regional_summary = sales_df.groupBy("region").agg(
    count("*").alias("total_orders"),
    sum("total_amount").alias("total_revenue"),
    avg("total_amount").alias("avg_order_value"),
    countDistinct("customer_id").alias("unique_customers")
).orderBy(desc("total_revenue"))

print("🌍 Regional Performance Summary:")
regional_summary.show(truncate=False)

# Sales rep performance
rep_summary = sales_df.groupBy("sales_rep", "region").agg(
    count("*").alias("total_orders"),
    sum("total_amount").alias("total_revenue"),
    avg("total_amount").alias("avg_order_value")
).orderBy(desc("total_revenue"))

print("👤 Sales Rep Performance:")
rep_summary.show(truncate=False)

# Save all summaries
print("💾 Saving analytics summaries...")
product_summary.write.format("delta").mode('overwrite').option("mergeSchema", "true").saveAsTable('product_performance')
regional_summary.write.format("delta").mode('overwrite').option("mergeSchema", "true").saveAsTable('regional_performance') 
rep_summary.write.format("delta").mode('overwrite').option("mergeSchema", "true").saveAsTable('sales_rep_performance')

print("✅ Saved all analytics tables to lakehouse")

# Final summary
print("\\n🎉 Sample data generation completed successfully!")
print("=" * 60)
print("📋 SUMMARY OF CREATED TABLES:")
print("=" * 60)
print(f"📊 demo_sales: {sales_df.count()} transaction records")
print(f"👥 demo_customers: {customers_df.count()} customer records") 
print(f"📈 product_performance: {product_summary.count()} product analytics")
print(f"🌍 regional_performance: {regional_summary.count()} regional analytics")
print(f"👤 sales_rep_performance: {rep_summary.count()} sales rep analytics")
print("=" * 60)
print("✅ All tables are now available in your Fabric Lakehouse!")
print("🌐 View them in the Fabric portal or query with SQL/Spark")
`;

async function addCompleteSampleData() {
  try {
    console.log('\n🔄 Step 1: Creating optimized Livy session...');
    const sessionResponse = await sendMcpRequest('tools/call', {
      name: 'create-livy-session',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        lakehouseId: config.lakehouseId,
        sessionConfig: {
          kind: 'pyspark',
          name: 'complete-data-session',
          driverMemory: '28g',
          executorMemory: '14g',
          numExecutors: 2
        }
      }
    });
    
    console.log('✅ Session creation response:');
    const sessionContent = sessionResponse.result.content[0].text;
    console.log(sessionContent);
    
    const sessionData = JSON.parse(sessionContent);
    const sessionId = sessionData.id;
    console.log(`🎯 Created Livy session with ID: ${sessionId}`);
    
    // Wait longer for session to be ready with more resources
    console.log('\n⏳ Waiting for session to initialize...');
    await new Promise(resolve => setTimeout(resolve, 30000));
    
    console.log('\n📊 Step 2: Executing comprehensive sample data generation...');
    const dataResponse = await sendMcpRequest('tools/call', {
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
    
    console.log('✅ Data generation execution response:');
    const execContent = dataResponse.result.content[0].text;
    console.log(execContent);
    
    const execData = JSON.parse(execContent);
    const statementId = execData.id;
    
    console.log(`🎯 Statement ID: ${statementId}, Status: ${execData.state}`);
    
    // Wait for execution to complete
    console.log('\n⏳ Waiting for comprehensive data generation (this may take 2-3 minutes)...');
    await new Promise(resolve => setTimeout(resolve, 120000)); // 2 minutes
    
    console.log('\n🔍 Step 3: Checking execution results...');
    const resultResponse = await sendMcpRequest('tools/call', {
      name: 'get-livy-statement',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        lakehouseId: config.lakehouseId,
        sessionId: sessionId,
        statementId: statementId
      }
    });
    
    console.log('✅ Final execution results:');
    const resultContent = resultResponse.result.content[0].text;
    const resultData = JSON.parse(resultContent);
    
    console.log(`📊 Execution Status: ${resultData.state}`);
    if (resultData.output && resultData.output.data) {
      console.log('📋 Output:');
      console.log(resultData.output.data['text/plain']);
    }
    
    console.log('\n🎉 Complete sample data process finished!');
    console.log(`✅ Lakehouse: MCPDemoLakehouse (${config.lakehouseId})`);
    console.log('📋 Tables created:');
    console.log('  🔹 demo_sales (500 transaction records)');
    console.log('  🔹 demo_customers (100 customer records)');
    console.log('  🔹 product_performance (analytics by product)');
    console.log('  🔹 regional_performance (analytics by region)');
    console.log('  🔹 sales_rep_performance (analytics by sales rep)');
    console.log(`🌐 View in Fabric Portal: https://msit.powerbi.com/groups/${config.workspaceId}`);
    
  } catch (error) {
    console.error('❌ Error in comprehensive data generation:', error.message);
    console.error('Stack:', error.stack);
  } finally {
    console.log('\n🛑 Stopping MCP server...');
    mcpServer.kill();
    setTimeout(() => process.exit(0), 3000);
  }
}

// Handle MCP server startup
mcpServer.stderr.on('data', (data) => {
  const message = data.toString();
  console.log('🔧 MCP Server:', message.trim());
  
  if (message.includes('Microsoft Fabric Analytics MCP Server running')) {
    console.log('✅ MCP Server started successfully');
    setTimeout(addCompleteSampleData, 3000);
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
