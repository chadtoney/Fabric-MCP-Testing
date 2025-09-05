const { spawn } = require('child_process');

// Configuration
const config = {
  workspaceId: process.env.FABRIC_DEFAULT_WORKSPACE_ID || 'bcb44215-0e69-46d3-aac9-fb92fadcd982',
  bearerToken: 'azure_cli',
  lakehouseId: '5e6b33fe-1f33-419a-a954-bce697ccfe61'
};

console.log('📊 Creating Fresh Livy Session and Adding Sample Data');
console.log(`Workspace: ${config.workspaceId}`);
console.log(`Lakehouse: ${config.lakehouseId}`);

// Simple but comprehensive data code
const sampleDataCode = `
print("🚀 Creating sample data for Fabric Lakehouse...")

from pyspark.sql import Row
from pyspark.sql.functions import *
import random

# Create sales data
print("📊 Creating sales dataset...")
sales_data = []

products = ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones"]
regions = ["North", "South", "East", "West"]

for i in range(50):  # Start with smaller dataset
    sales_data.append(Row(
        sale_id=i + 1,
        product=products[i % len(products)],
        region=regions[i % len(regions)],
        quantity=random.randint(1, 5),
        unit_price=100.0 + (i * 10),
        total_amount=(random.randint(1, 5) * (100.0 + (i * 10)))
    ))

# Create DataFrame
sales_df = spark.createDataFrame(sales_data)
print(f"✅ Created {sales_df.count()} sales records")

# Show sample
print("📋 Sample data:")
sales_df.show(5)

# Save to lakehouse
sales_df.write.format("delta").mode("overwrite").saveAsTable("demo_sales")
print("✅ Saved demo_sales table to lakehouse")

# Create summary
summary_df = sales_df.groupBy("product").agg(
    count("*").alias("orders"),
    sum("total_amount").alias("revenue")
)

print("📈 Product summary:")
summary_df.show()

summary_df.write.format("delta").mode("overwrite").saveAsTable("product_summary")
print("✅ Saved product_summary table to lakehouse")

print("🎉 Sample data creation completed successfully!")
print("Tables created: demo_sales, product_summary")
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

async function createFreshSessionAndAddData() {
  try {
    console.log('\n🔄 Step 1: Creating a fresh Livy session...');
    const sessionResponse = await sendMcpRequest('tools/call', {
      name: 'create-livy-session',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        lakehouseId: config.lakehouseId,
        sessionConfig: {
          kind: 'pyspark',
          name: 'fresh-sample-data-session',
          driverMemory: '14g',  // Use smaller memory allocation
          executorMemory: '7g',
          numExecutors: 1
        }
      }
    });
    
    if (sessionResponse.error) {
      console.error('❌ Session creation error:', sessionResponse.error.message);
      return;
    }
    
    const sessionContent = sessionResponse.result.content[0].text;
    const sessionData = JSON.parse(sessionContent);
    const sessionId = parseInt(sessionData.id);
    
    console.log(`✅ Fresh session created with ID: ${sessionId}`);
    
    // Wait longer for fresh session to initialize
    console.log('\n⏳ Waiting 45 seconds for fresh session to fully initialize...');
    await new Promise(resolve => setTimeout(resolve, 45000));
    
    console.log('\n📊 Step 2: Executing sample data creation...');
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
    
    if (dataResponse.error) {
      console.error('❌ Execution error:', dataResponse.error.message);
      return;
    }
    
    const execContent = dataResponse.result.content[0].text;
    
    let statementId;
    try {
      const execData = JSON.parse(execContent);
      statementId = execData.id;
      console.log(`✅ Code submitted with statement ID: ${statementId}, Status: ${execData.state}`);
    } catch (parseError) {
      console.log('❌ Could not parse execution response');
      console.log('Raw response:', execContent);
      return;
    }
    
    // Wait for execution
    console.log('\n⏳ Waiting 60 seconds for data creation...');
    await new Promise(resolve => setTimeout(resolve, 60000));
    
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
    
    if (resultResponse.error) {
      console.log('❌ Error getting results:', resultResponse.error.message);
    } else {
      const resultContent = resultResponse.result.content[0].text;
      const resultData = JSON.parse(resultContent);
      
      console.log(`📊 Execution Status: ${resultData.state}`);
      
      if (resultData.output && resultData.output.data) {
        console.log('📋 Execution Output:');
        console.log(resultData.output.data['text/plain']);
        
        if (resultData.state === 'available' && resultData.output.data['text/plain'].includes('completed successfully')) {
          console.log('\n🎉 SUCCESS! Sample data has been created in the lakehouse!');
          console.log('✅ Tables created: demo_sales, product_summary');
        }
      } else {
        console.log('ℹ️ No output data available yet');
      }
    }
    
    console.log('\n📊 Final Summary:');
    console.log(`✅ Lakehouse: MCPDemoLakehouse (${config.lakehouseId})`);
    console.log('📋 Expected tables: demo_sales (50 records), product_summary');
    console.log(`🌐 View in portal: https://msit.powerbi.com/groups/${config.workspaceId}`);
    
  } catch (error) {
    console.error('❌ Error in fresh session approach:', error.message);
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
    setTimeout(createFreshSessionAndAddData, 3000);
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
