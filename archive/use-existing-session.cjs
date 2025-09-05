const { spawn } = require('child_process');

// Configuration
const config = {
  workspaceId: process.env.FABRIC_DEFAULT_WORKSPACE_ID || 'bcb44215-0e69-46d3-aac9-fb92fadcd982',
  bearerToken: 'azure_cli',
  lakehouseId: '5e6b33fe-1f33-419a-a954-bce697ccfe61',
  existingSessionId: 72 // Use the reliable-session that's already running
};

console.log('📊 Using Existing Session to Add Sample Data to Fabric Lakehouse');
console.log(`Workspace: ${config.workspaceId}`);
console.log(`Lakehouse: ${config.lakehouseId}`);
console.log(`Using existing session ID: ${config.existingSessionId}`);

// Simple, working data generation script
const finalDataCode = `
print("🚀 Starting final sample data generation using existing session...")

# Import libraries
from pyspark.sql import Row
from pyspark.sql.functions import *
import random

print("📊 Creating comprehensive sales dataset...")

# Create realistic sales data
sales_data = []
products = [
    "Laptop Pro 15", "Wireless Mouse", "Mechanical Keyboard", "4K Monitor",
    "Gaming Headphones", "Tablet Pro", "Smartphone Plus", "Smart Watch",
    "USB-C Hub", "Webcam HD"
]
regions = ["North America", "Europe", "Asia Pacific", "South America"]
sales_reps = ["Alice Johnson", "Bob Smith", "Carol Williams", "David Brown", "Emma Davis"]
categories = ["Electronics", "Accessories", "Displays", "Audio", "Wearables"]

for i in range(200):
    product = products[i % len(products)]
    category = categories[i % len(categories)]
    region = regions[i % len(regions)]
    rep = sales_reps[i % len(sales_reps)]
    quantity = random.randint(1, 8)
    base_price = 50 + (i * 10.0)
    unit_price = round(base_price * (0.8 + random.random() * 0.4), 2)
    total = round(quantity * unit_price, 2)
    
    sales_data.append(Row(
        order_id=f"ORD-{10000 + i}",
        product_name=product,
        category=category,
        region=region,
        sales_rep=rep,
        quantity=quantity,
        unit_price=unit_price,
        total_amount=total,
        customer_id=f"CUST-{5000 + (i % 100)}"
    ))

# Create DataFrame
sales_df = spark.createDataFrame(sales_data)
print(f"✅ Created sales dataset with {sales_df.count()} records")

# Show sample
print("📋 Sample sales data:")
sales_df.show(5, truncate=False)

# Save to delta table
print("💾 Saving to demo_sales table...")
sales_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("demo_sales")
print("✅ Successfully saved demo_sales table")

# Create customer data
print("👥 Creating customer dataset...")
customer_data = []
companies = ["TechCorp", "GlobalSoft", "InnovateLabs", "DigitalDyn", "FutureSys"]
titles = ["CEO", "CTO", "Manager", "Developer", "Analyst"]

for i in range(100):
    customer_data.append(Row(
        customer_id=f"CUST-{5000 + i}",
        customer_name=f"Customer {i+1}",
        company=companies[i % len(companies)],
        title=titles[i % len(titles)],
        region=regions[i % len(regions)]
    ))

customers_df = spark.createDataFrame(customer_data)
print(f"✅ Created customers dataset with {customers_df.count()} records")

customers_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("demo_customers")
print("✅ Successfully saved demo_customers table")

# Create analytics
print("📈 Creating analytics summaries...")

# Product summary
product_summary = sales_df.groupBy("product_name", "category").agg(
    count("*").alias("total_orders"),
    sum("quantity").alias("total_units"),
    sum("total_amount").alias("total_revenue"),
    round(avg("total_amount"), 2).alias("avg_order_value")
).orderBy(desc("total_revenue"))

print("📊 Product Performance:")
product_summary.show(10, truncate=False)

product_summary.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("product_performance")
print("✅ Successfully saved product_performance table")

# Regional summary
regional_summary = sales_df.groupBy("region").agg(
    count("*").alias("total_orders"),
    sum("total_amount").alias("total_revenue"),
    countDistinct("customer_id").alias("unique_customers")
).orderBy(desc("total_revenue"))

print("🌍 Regional Performance:")
regional_summary.show(truncate=False)

regional_summary.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("regional_performance")
print("✅ Successfully saved regional_performance table")

print("")
print("=" * 60)
print("🎉 SAMPLE DATA GENERATION COMPLETED SUCCESSFULLY!")
print("=" * 60)
print("📋 TABLES CREATED:")
print(f"  📊 demo_sales: {sales_df.count()} transaction records")
print(f"  👥 demo_customers: {customers_df.count()} customer records")
print(f"  📈 product_performance: {product_summary.count()} product analytics")
print(f"  🌍 regional_performance: {regional_summary.count()} regional analytics")
print("=" * 60)
print("✅ All tables saved as Delta format in your Fabric Lakehouse!")
print("🌐 View them in the Fabric portal or query with SQL/Spark")
print("📊 Ready for Power BI dashboards and advanced analytics!")
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
      reject(new Error(`Request ${method} timed out after 60 seconds`));
    }, 60000);
    
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

async function useExistingSessionForData() {
  try {
    console.log('\n📊 Step 1: Executing comprehensive data generation using existing session...');
    const dataResponse = await sendMcpRequest('tools/call', {
      name: 'execute-livy-statement',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        lakehouseId: config.lakehouseId,
        sessionId: config.existingSessionId,
        code: finalDataCode,
        kind: 'pyspark'
      }
    });
    
    if (dataResponse.error) {
      console.error('❌ Execution error:', dataResponse.error.message);
      return;
    }
    
    const execContent = dataResponse.result.content[0].text;
    console.log('✅ Execution response:', execContent);
    
    let statementId;
    try {
      const execData = JSON.parse(execContent);
      statementId = execData.id;
      console.log(`🎯 Statement submitted with ID: ${statementId}, Status: ${execData.state}`);
    } catch (parseError) {
      console.log('❌ Could not parse execution response');
      console.log('Raw content:', execContent);
      return;
    }
    
    // Wait for execution
    console.log('\n⏳ Waiting 90 seconds for comprehensive data generation...');
    await new Promise(resolve => setTimeout(resolve, 90000));
    
    console.log('\n🔍 Step 2: Checking execution results...');
    const resultResponse = await sendMcpRequest('tools/call', {
      name: 'get-livy-statement',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        lakehouseId: config.lakehouseId,
        sessionId: config.existingSessionId,
        statementId: statementId
      }
    });
    
    if (resultResponse.error) {
      console.log('❌ Error getting results:', resultResponse.error.message);
    } else {
      const resultContent = resultResponse.result.content[0].text;
      console.log('✅ Raw result response:', resultContent);
      
      try {
        const resultData = JSON.parse(resultContent);
        console.log(`📊 Final Execution Status: ${resultData.state}`);
        
        if (resultData.output && resultData.output.data) {
          console.log('📋 Execution Output:');
          console.log(resultData.output.data['text/plain']);
        }
        
        if (resultData.state === 'available') {
          console.log('🎉 SUCCESS: Data generation completed successfully!');
        } else {
          console.log(`ℹ️ Execution state: ${resultData.state}`);
        }
      } catch (parseError) {
        console.log('Raw result content:', resultContent);
      }
    }
    
    console.log('\n🎉 Sample data process completed!');
    console.log(`✅ Lakehouse: MCPDemoLakehouse (${config.lakehouseId})`);
    console.log('📋 Expected tables created:');
    console.log('  🔹 demo_sales (200 transaction records)');
    console.log('  🔹 demo_customers (100 customer records)');
    console.log('  🔹 product_performance (product analytics)');
    console.log('  🔹 regional_performance (regional analytics)');
    console.log(`🌐 View in Fabric Portal: https://msit.powerbi.com/groups/${config.workspaceId}`);
    console.log('📊 Ready for Power BI reports and advanced analytics!');
    
  } catch (error) {
    console.error('❌ Error using existing session:', error.message);
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
    setTimeout(useExistingSessionForData, 2000);
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
