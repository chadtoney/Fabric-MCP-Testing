const { spawn } = require('child_process');

// Configuration
const config = {
  workspaceId: process.env.FABRIC_DEFAULT_WORKSPACE_ID || 'bcb44215-0e69-46d3-aac9-fb92fadcd982',
  bearerToken: 'azure_cli',
  lakehouseId: '5e6b33fe-1f33-419a-a954-bce697ccfe61' // ID from the created lakehouse
};

console.log('📊 Adding Sample Data to Microsoft Fabric Lakehouse (Simple & Reliable)');
console.log(`Workspace: ${config.workspaceId}`);
console.log(`Lakehouse: ${config.lakehouseId}`);

// Simple, reliable data generation script
const reliableDataCode = `
print("🚀 Starting reliable sample data generation...")

# Import libraries
from pyspark.sql import Row
from pyspark.sql.functions import *
import random

print("📊 Creating sales data...")

# Create simple sales dataset
sales_data = []
products = ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones', 'Tablet', 'Phone', 'Watch']
regions = ['North', 'South', 'East', 'West']
sales_reps = ['Alice', 'Bob', 'Carol', 'David']

for i in range(100):
    product = products[i % len(products)]
    region = regions[i % len(regions)]
    rep = sales_reps[i % len(sales_reps)]
    quantity = random.randint(1, 5)
    price = 100 + (i * 5.0)
    total = quantity * price
    
    sales_data.append(Row(
        id=i + 1,
        product=product,
        region=region,
        sales_rep=rep,
        quantity=quantity,
        unit_price=price,
        total_amount=total
    ))

# Create DataFrame
sales_df = spark.createDataFrame(sales_data)
print(f"✅ Created {sales_df.count()} sales records")

# Show sample
print("📋 Sample data:")
sales_df.show(5)

# Save to table
try:
    sales_df.write.format("delta").mode('overwrite').saveAsTable('demo_sales')
    print("✅ Successfully saved demo_sales table")
except Exception as e:
    print(f"❌ Error saving table: {e}")

# Create summary
print("📈 Creating summary...")
summary_df = sales_df.groupBy("product").agg(
    count("*").alias("orders"),
    sum("total_amount").alias("revenue")
)

print("📊 Product summary:")
summary_df.show()

try:
    summary_df.write.format("delta").mode('overwrite').saveAsTable('product_summary')
    print("✅ Successfully saved product_summary table")
except Exception as e:
    print(f"❌ Error saving summary: {e}")

print("🎉 Data generation completed!")
print("Created tables: demo_sales, product_summary")
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
      reject(new Error(`Request ${method} timed out after 45 seconds`));
    }, 45000);
    
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

async function addReliableSampleData() {
  try {
    console.log('\n🔄 Step 1: Creating basic Livy session...');
    const sessionResponse = await sendMcpRequest('tools/call', {
      name: 'create-livy-session',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        lakehouseId: config.lakehouseId,
        sessionConfig: {
          kind: 'pyspark',
          name: 'reliable-session'
        }
      }
    });
    
    if (sessionResponse.error) {
      throw new Error(`Session creation failed: ${sessionResponse.error.message}`);
    }
    
    const sessionContent = sessionResponse.result.content[0].text;
    const sessionData = JSON.parse(sessionContent);
    const sessionId = parseInt(sessionData.id);
    
    console.log(`✅ Session created with ID: ${sessionId}`);
    
    // Wait for session to be ready
    console.log('\n⏳ Waiting 20 seconds for session to start...');
    await new Promise(resolve => setTimeout(resolve, 20000));
    
    console.log('\n📊 Step 2: Executing reliable data generation...');
    const dataResponse = await sendMcpRequest('tools/call', {
      name: 'execute-livy-statement',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        lakehouseId: config.lakehouseId,
        sessionId: sessionId,
        code: reliableDataCode,
        kind: 'pyspark'
      }
    });
    
    if (dataResponse.error) {
      throw new Error(`Code execution failed: ${dataResponse.error.message}`);
    }
    
    let execContent, execData, statementId;
    
    try {
      execContent = dataResponse.result.content[0].text;
      execData = JSON.parse(execContent);
      statementId = execData.id;
      console.log(`✅ Code submitted with statement ID: ${statementId}, Status: ${execData.state}`);
    } catch (parseError) {
      console.log('❌ Could not parse execution response');
      console.log('Raw response:', execContent || 'No content');
      throw parseError;
    }
    
    // Wait for execution
    console.log('\n⏳ Waiting 45 seconds for code execution...');
    await new Promise(resolve => setTimeout(resolve, 45000));
    
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
      
      console.log(`📊 Final Status: ${resultData.state}`);
      
      if (resultData.output && resultData.output.data) {
        console.log('📋 Execution Output:');
        console.log(resultData.output.data['text/plain']);
      } else {
        console.log('ℹ️ No output data available');
      }
    }
    
    console.log('\n🎉 Reliable sample data process completed!');
    console.log(`✅ Lakehouse: MCPDemoLakehouse (${config.lakehouseId})`);
    console.log('📋 Expected tables:');
    console.log('  - demo_sales (100 sales records)');
    console.log('  - product_summary (aggregated by product)');
    console.log(`🌐 View in Fabric Portal: https://msit.powerbi.com/groups/${config.workspaceId}`);
    
  } catch (error) {
    console.error('❌ Error in reliable data generation:', error.message);
    console.error('Full error details:', error);
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
    setTimeout(addReliableSampleData, 2000);
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
