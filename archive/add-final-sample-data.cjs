const { spawn } = require('child_process');

// Configuration
const config = {
  workspaceId: process.env.FABRIC_DEFAULT_WORKSPACE_ID || 'bcb44215-0e69-46d3-aac9-fb92fadcd982',
  bearerToken: 'azure_cli',
  lakehouseId: '5e6b33fe-1f33-419a-a954-bce697ccfe61' // ID from the created lakehouse
};

console.log('📊 Adding Sample Data to Microsoft Fabric Lakehouse via MCP (Fixed SessionID)');
console.log(`Workspace: ${config.workspaceId}`);
console.log(`Lakehouse: ${config.lakehouseId}`);

// Comprehensive sample data generation script
const sampleDataCode = `
print("🚀 Starting comprehensive sample data generation for Fabric Lakehouse...")

# Import required libraries
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

print("📊 Creating comprehensive sales dataset...")

# Sample data arrays
products = [
    {'name': 'Laptop Pro 15', 'category': 'Electronics', 'price': 1299.99},
    {'name': 'Wireless Mouse', 'category': 'Accessories', 'price': 79.99},
    {'name': 'Mechanical Keyboard', 'category': 'Accessories', 'price': 149.99},
    {'name': '4K Monitor 27"', 'category': 'Displays', 'price': 899.99},
    {'name': 'Gaming Headphones', 'category': 'Audio', 'price': 199.99},
    {'name': 'Tablet Pro', 'category': 'Electronics', 'price': 699.99},
    {'name': 'Smartphone Plus', 'category': 'Electronics', 'price': 999.99},
    {'name': 'Smart Watch', 'category': 'Wearables', 'price': 399.99}
]

regions = ['North America', 'Europe', 'Asia Pacific', 'South America']
sales_reps = ['Alice Johnson', 'Bob Smith', 'Carol Williams', 'David Brown', 'Emma Davis', 'Frank Miller']

# Generate 300 sales records
sales_data = []
for i in range(300):
    product = products[i % len(products)]
    order_date = datetime.now() - timedelta(days=random.randint(1, 365))
    quantity = random.randint(1, 8)
    # Add some price variation
    unit_price = round(product['price'] * (0.85 + random.random() * 0.3), 2)
    total_amount = round(quantity * unit_price, 2)
    
    sales_data.append(Row(
        order_id=f"ORD-{10000 + i}",
        order_date=order_date.strftime("%Y-%m-%d"),
        product_name=product['name'],
        product_category=product['category'],
        quantity=quantity,
        unit_price=unit_price,
        total_amount=total_amount,
        region=regions[i % len(regions)],
        sales_rep=sales_reps[i % len(sales_reps)],
        customer_id=f"CUST-{random.randint(5000, 9999)}"
    ))

# Create sales DataFrame
sales_df = spark.createDataFrame(sales_data)
print(f"✅ Created sales dataset with {sales_df.count()} records")

# Show sample data
print("📋 Sample sales data (first 10 rows):")
sales_df.show(10, truncate=False)

# Save sales data to lakehouse
print("💾 Saving sales data to demo_sales table...")
sales_df.write.format("delta").mode('overwrite').option("mergeSchema", "true").saveAsTable('demo_sales')
print("✅ Saved demo_sales table to lakehouse")

# Create customer dataset
print("👥 Creating customer dataset...")
customer_data = []
companies = ['TechCorp Inc', 'Global Solutions Ltd', 'Innovation Labs', 'Digital Dynamics', 'Future Systems', 'Smart Enterprises']
job_titles = ['CEO', 'CTO', 'IT Manager', 'Software Developer', 'Business Analyst', 'Project Manager', 'Data Scientist']

for i in range(150):
    customer_data.append(Row(
        customer_id=f"CUST-{5000 + i}",
        customer_name=f"Customer {i+1}",
        company=companies[i % len(companies)],
        job_title=job_titles[i % len(job_titles)],
        email=f"customer{i+1}@{companies[i % len(companies)].lower().replace(' ', '').replace('ltd', 'com').replace('inc', 'com')}",
        phone=f"+1-555-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
        region=regions[i % len(regions)],
        registration_date=(datetime.now() - timedelta(days=random.randint(30, 800))).strftime("%Y-%m-%d"),
        customer_tier=(['Bronze', 'Silver', 'Gold', 'Platinum'][i % 4])
    ))

# Create customers DataFrame
customers_df = spark.createDataFrame(customer_data)
print(f"✅ Created customers dataset with {customers_df.count()} records")

# Show sample customer data
print("📋 Sample customer data (first 5 rows):")
customers_df.show(5, truncate=False)

# Save customer data
print("💾 Saving customer data to demo_customers table...")
customers_df.write.format("delta").mode('overwrite').option("mergeSchema", "true").saveAsTable('demo_customers')
print("✅ Saved demo_customers table to lakehouse")

# Create analytics summaries
print("📈 Creating analytics summaries...")

# Product performance summary
product_summary = sales_df.groupBy("product_name", "product_category").agg(
    count("*").alias("total_orders"),
    sum("quantity").alias("total_units_sold"),
    sum("total_amount").alias("total_revenue"),
    avg("total_amount").alias("avg_order_value"),
    round(avg("unit_price"), 2).alias("avg_unit_price")
).orderBy(desc("total_revenue"))

print("📊 Product Performance Summary:")
product_summary.show(truncate=False)

# Regional performance summary
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
    avg("total_amount").alias("avg_order_value"),
    round(sum("total_amount") / count("*"), 2).alias("revenue_per_order")
).orderBy(desc("total_revenue"))

print("👤 Sales Rep Performance:")
rep_summary.show(truncate=False)

# Monthly trends
monthly_summary = sales_df.withColumn("order_month", date_format(col("order_date"), "yyyy-MM")).groupBy("order_month").agg(
    count("*").alias("total_orders"),
    sum("total_amount").alias("total_revenue"),
    countDistinct("customer_id").alias("unique_customers")
).orderBy("order_month")

print("📅 Monthly Sales Trends:")
monthly_summary.show()

# Save all analytics tables
print("💾 Saving analytics summaries...")
product_summary.write.format("delta").mode('overwrite').option("mergeSchema", "true").saveAsTable('product_performance')
regional_summary.write.format("delta").mode('overwrite').option("mergeSchema", "true").saveAsTable('regional_performance')
rep_summary.write.format("delta").mode('overwrite').option("mergeSchema", "true").saveAsTable('sales_rep_performance')
monthly_summary.write.format("delta").mode('overwrite').option("mergeSchema", "true").saveAsTable('monthly_trends')

print("✅ Saved all analytics tables to lakehouse")

print("\\n" + "="*70)
print("🎉 COMPREHENSIVE SAMPLE DATA GENERATION COMPLETED SUCCESSFULLY!")
print("="*70)
print("📋 SUMMARY OF CREATED TABLES:")
print(f"  📊 demo_sales: {sales_df.count()} transaction records")
print(f"  👥 demo_customers: {customers_df.count()} customer records")
print(f"  📈 product_performance: {product_summary.count()} product analytics")
print(f"  🌍 regional_performance: {regional_summary.count()} regional analytics")
print(f"  👤 sales_rep_performance: {rep_summary.count()} sales rep analytics")
print(f"  📅 monthly_trends: {monthly_summary.count()} monthly trends")
print("="*70)
print("✅ All tables are now available in your Microsoft Fabric Lakehouse!")
print("🌐 Access them via the Fabric portal or query with SQL/Spark")
print("📊 Ready for Power BI reports, data analysis, and machine learning!")
`;

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
      reject(new Error(`Request ${method} timed out after 75 seconds`));
    }, 75000);
    
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

async function addComprehensiveSampleData() {
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
          name: 'comprehensive-data-session',
          driverMemory: '28g',
          executorMemory: '14g',
          numExecutors: 2
        }
      }
    });
    
    console.log('✅ Session created successfully');
    const sessionContent = sessionResponse.result.content[0].text;
    const sessionData = JSON.parse(sessionContent);
    const sessionId = parseInt(sessionData.id); // Convert to number!
    
    console.log(`🎯 Session ID (as number): ${sessionId}`);
    
    // Wait for session to initialize
    console.log('\n⏳ Waiting 25 seconds for session to start...');
    await new Promise(resolve => setTimeout(resolve, 25000));
    
    console.log('\n📊 Step 2: Executing comprehensive data generation...');
    const dataResponse = await sendMcpRequest('tools/call', {
      name: 'execute-livy-statement',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        lakehouseId: config.lakehouseId,
        sessionId: sessionId, // Now properly as number
        code: sampleDataCode,
        kind: 'pyspark'
      }
    });
    
    console.log('✅ Code execution initiated');
    const execContent = dataResponse.result.content[0].text;
    const execData = JSON.parse(execContent);
    const statementId = execData.id;
    
    console.log(`🎯 Statement ID: ${statementId}, Status: ${execData.state}`);
    
    // Wait for execution to complete (longer time for comprehensive data)
    console.log('\n⏳ Waiting for comprehensive data generation (this may take 2-3 minutes)...');
    await new Promise(resolve => setTimeout(resolve, 150000)); // 2.5 minutes
    
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
    
    console.log('✅ Execution completed - checking results');
    const resultContent = resultResponse.result.content[0].text;
    const resultData = JSON.parse(resultContent);
    
    console.log(`📊 Final Execution Status: ${resultData.state}`);
    if (resultData.output && resultData.output.data) {
      console.log('📋 Execution Output:');
      console.log(resultData.output.data['text/plain']);
    }
    
    console.log('\n🎉 COMPREHENSIVE SAMPLE DATA GENERATION COMPLETED!');
    console.log(`✅ Lakehouse: MCPDemoLakehouse (${config.lakehouseId})`);
    console.log('📋 Tables created:');
    console.log('  🔹 demo_sales (300 transaction records)');
    console.log('  🔹 demo_customers (150 customer records)');
    console.log('  🔹 product_performance (product analytics)');
    console.log('  🔹 regional_performance (regional analytics)');
    console.log('  🔹 sales_rep_performance (sales rep analytics)');
    console.log('  🔹 monthly_trends (time-based analytics)');
    console.log(`🌐 View in Fabric Portal: https://msit.powerbi.com/groups/${config.workspaceId}`);
    console.log('📊 Ready for Power BI dashboards and advanced analytics!');
    
  } catch (error) {
    console.error('❌ Error in comprehensive data generation:', error.message);
    if (error.response) {
      console.error('Response:', JSON.stringify(error.response, null, 2));
    }
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
    setTimeout(addComprehensiveSampleData, 3000);
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
