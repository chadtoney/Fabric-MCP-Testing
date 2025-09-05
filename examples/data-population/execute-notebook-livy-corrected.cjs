const { spawn } = require('child_process');

// Configuration - Update these values for your environment
const config = {
  // Get your workspace ID from Fabric portal URL: https://app.fabric.microsoft.com/groups/YOUR-WORKSPACE-ID
  workspaceId: process.env.FABRIC_DEFAULT_WORKSPACE_ID || 'YOUR_WORKSPACE_ID_HERE',
  // Lakehouse ID will be discovered automatically, or specify if known
  lakehouseId: process.env.FABRIC_LAKEHOUSE_ID || 'MCPDemoLakehouse', 
  bearerToken: 'azure_cli' // Use Azure CLI authentication
};

// Path to the Fabric MCP Server - update this to your installation location
const MCP_SERVER_PATH = process.env.FABRIC_MCP_SERVER_PATH || 'C:\\Repos\\Fabric-Analytics-MCP\\build\\index.js';

console.log('🚀 Creating Livy session and executing notebook code...');
console.log(`Workspace: ${config.workspaceId}`);
console.log(`Lakehouse: ${config.lakehouseId}`);

if (config.workspaceId === 'YOUR_WORKSPACE_ID_HERE') {
  console.log('❌ Please update config.workspaceId with your actual workspace ID');
  console.log('💡 Find it in your Fabric portal URL: https://app.fabric.microsoft.com/groups/YOUR-WORKSPACE-ID');
  process.exit(1);
}

// Start the MCP server process
const mcpServer = spawn('node', [MCP_SERVER_PATH], {
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
  
  console.log(`📤 Sending: ${JSON.stringify(request, null, 100)}`);
  mcpServer.stdin.write(JSON.stringify(request) + '\n');
  
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error('Request timeout'));
    }, 120000); // 2 minutes for long operations
    
    function handleResponse(data) {
      responseBuffer += data;
      
      const lines = responseBuffer.split('\n');
      responseBuffer = lines.pop() || '';
      
      for (const line of lines) {
        if (!line.trim()) continue;
        
        try {
          const response = JSON.parse(line);
          if (response.id === request.id) {
            clearTimeout(timeout);
            mcpServer.stdout.removeListener('data', handleResponse);
            console.log(`📥 Received: ${JSON.stringify(response, null, 2)}`);
            resolve(response);
            return;
          }
        } catch (err) {
          // Ignore parsing errors for intermediate messages
        }
      }
    }
    
    mcpServer.stdout.on('data', handleResponse);
  });
}

async function executeNotebookViaLivy() {
  let sessionId = null;
  
  try {
    // Wait for server to start
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    console.log('\n🚀 Step 1: Creating Livy session...');
    const sessionResponse = await sendMcpRequest('tools/call', {
      name: 'create-livy-session',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        lakehouseId: config.lakehouseId,
        sessionConfig: {
          kind: 'pyspark'
        }
      }
    });
    
    if (sessionResponse.result && sessionResponse.result.content[0].text) {
      // Parse the JSON response to extract session ID
      try {
        const responseText = sessionResponse.result.content[0].text;
        const sessionData = JSON.parse(responseText);
        if (sessionData.id) {
          sessionId = sessionData.id; // This is a string UUID
          console.log(`✅ Created Livy session: ${sessionId}`);
          
          // Wait for session to be ready
          console.log('\n⏳ Waiting for session to be ready...');
          await new Promise(resolve => setTimeout(resolve, 30000)); // Wait 30 seconds
          
          // Execute the first cell - imports and setup
          console.log('\n🚀 Step 2: Executing imports and setup...');
          const setupCode = `
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

# Initialize Spark session
spark = SparkSession.builder.appName("MCPDemoDataPopulation").getOrCreate()
print("✅ Spark session initialized successfully")
print(f"Spark version: {spark.version}")

# Show available databases
spark.sql("SHOW DATABASES").show()
print("✅ Setup completed")
`;
          
          const setupResponse = await sendMcpRequest('tools/call', {
            name: 'execute-livy-statement',
            arguments: {
              bearerToken: config.bearerToken,
              workspaceId: config.workspaceId,
              lakehouseId: config.lakehouseId,
              sessionId: sessionId,
              code: setupCode,
              kind: 'pyspark'
            }
          });
          
          console.log('\n📋 Setup execution result:');
          console.log(JSON.stringify(setupResponse, null, 2));
          
          // If setup was successful, continue with data generation
          if (setupResponse.result && !setupResponse.result.content[0].text.includes('Error')) {
            console.log('\n🚀 Step 3: Creating demo sales data...');
            
            const salesDataCode = `
# Generate demo sales data
print("🔄 Generating demo sales data...")

# Define schema for sales data
sales_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("total_amount", DoubleType(), False),
    StructField("transaction_date", DateType(), False),
    StructField("store_location", StringType(), False),
    StructField("sales_channel", StringType(), False),
    StructField("payment_method", StringType(), False)
])

# Sample data for generation
categories = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Food & Beverage"]
locations = ["New York", "California", "Texas", "Florida", "Illinois", "Washington"]
channels = ["Online", "In-Store", "Mobile App", "Phone"]
payment_methods = ["Credit Card", "Debit Card", "Cash", "Digital Wallet", "Gift Card"]

# Generate sample data
sample_data = []
base_date = datetime(2024, 1, 1)

print("🔄 Generating 1000 sales records...")

for i in range(1000):
    transaction_id = f"TXN{str(i+1).zfill(6)}"
    customer_id = f"CUST{str(random.randint(1, 500)).zfill(4)}"
    product_id = f"PROD{str(random.randint(1, 200)).zfill(3)}"
    product_name = f"Product {product_id}"
    category = random.choice(categories)
    quantity = random.randint(1, 10)
    unit_price = round(random.uniform(10.0, 500.0), 2)
    total_amount = round(quantity * unit_price, 2)
    
    # Random date within the last year
    days_offset = random.randint(0, 365)
    transaction_date = base_date + timedelta(days=days_offset)
    
    store_location = random.choice(locations)
    sales_channel = random.choice(channels)
    payment_method = random.choice(payment_methods)
    
    sample_data.append((
        transaction_id, customer_id, product_id, product_name, category,
        quantity, unit_price, total_amount, transaction_date.date(),
        store_location, sales_channel, payment_method
    ))

# Create DataFrame
sales_df = spark.createDataFrame(sample_data, sales_schema)

print(f"✅ Created sales DataFrame with {sales_df.count()} records")

# Write to delta table
sales_df.write.format("delta").mode("overwrite").saveAsTable("demo_sales")
print("✅ Saved sales data to demo_sales table")

# Show sample of the data
print("📊 Sample of generated sales data:")
sales_df.show(10)
`;
            
            const salesResponse = await sendMcpRequest('tools/call', {
              name: 'execute-livy-statement',
              arguments: {
                bearerToken: config.bearerToken,
                workspaceId: config.workspaceId,
                lakehouseId: config.lakehouseId,
                sessionId: sessionId,
                code: salesDataCode,
                kind: 'pyspark'
              }
            });
            
            console.log('\n📋 Sales data creation result:');
            console.log(JSON.stringify(salesResponse, null, 2));
          }
        }
      } catch (parseError) {
        console.log('❌ Failed to parse session response:', parseError.message);
        console.log('Raw response:', sessionResponse.result.content[0].text);
      }
    } else {
      console.log('❌ Failed to create Livy session');
      console.log('Response:', sessionResponse);
    }
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  } finally {
    // Clean up session if created
    if (sessionId) {
      try {
        console.log(`\n🧹 Cleaning up session ${sessionId}...`);
        await sendMcpRequest('tools/call', {
          name: 'delete-livy-session',
          arguments: {
            bearerToken: config.bearerToken,
            workspaceId: config.workspaceId,
            lakehouseId: config.lakehouseId,
            sessionId: sessionId
          }
        });
      } catch (err) {
        console.error('Failed to clean up session:', err.message);
      }
    }
    
    mcpServer.kill();
    console.log('\n🏁 Process completed');
  }
}

// Handle server errors
mcpServer.stderr.on('data', (data) => {
  const output = data.toString();
  if (output.includes('Microsoft Fabric Analytics MCP Server running')) {
    console.log('✅ MCP Server is ready');
  } else if (output.includes('error') || output.includes('Error')) {
    console.error('Server error:', output);
  }
});

mcpServer.on('close', (code) => {
  console.log(`Server process exited with code ${code}`);
});

// Start the process
executeNotebookViaLivy();
