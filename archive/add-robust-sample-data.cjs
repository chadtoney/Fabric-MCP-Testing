const { spawn } = require('child_process');

// Configuration
const config = {
  workspaceId: process.env.FABRIC_DEFAULT_WORKSPACE_ID || 'bcb44215-0e69-46d3-aac9-fb92fadcd982',
  bearerToken: 'azure_cli',
  lakehouseId: '5e6b33fe-1f33-419a-a954-bce697ccfe61' // ID from the created lakehouse
};

console.log('📊 Adding Sample Data to Microsoft Fabric Lakehouse via MCP (Robust)');
console.log(`Workspace: ${config.workspaceId}`);
console.log(`Lakehouse: ${config.lakehouseId}`);

// Simple but comprehensive data generation script
const robustDataCode = `
print("🚀 Starting robust sample data generation...")

from pyspark.sql import Row
from pyspark.sql.functions import *
import random

# Create sales data
print("📊 Creating sales transactions...")
sales_data = []
products = ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones']
regions = ['North', 'South', 'East', 'West'] 

for i in range(200):
    sales_data.append(Row(
        sale_id=i + 1,
        product=products[i % len(products)],
        quantity=random.randint(1, 5),
        unit_price=round(100 + (i * 2.5), 2),
        region=regions[i % len(regions)],
        total_amount=round((random.randint(1, 5) * (100 + (i * 2.5))), 2)
    ))

# Create DataFrame and save
sales_df = spark.createDataFrame(sales_data)
print(f"✅ Created {sales_df.count()} sales records")

sales_df.write.format("delta").mode('overwrite').saveAsTable('demo_sales')
print("✅ Saved demo_sales table")

# Create summary
summary_df = sales_df.groupBy("product", "region").agg(
    count("*").alias("total_sales"),
    sum("total_amount").alias("total_revenue")
)

summary_df.write.format("delta").mode('overwrite').saveAsTable('sales_summary')
print("✅ Saved sales_summary table")

print("🎉 Data generation completed!")
print("Tables created: demo_sales, sales_summary")
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

async function addRobustSampleData() {
  try {
    console.log('\n🔄 Step 1: Creating simple Livy session...');
    const sessionResponse = await sendMcpRequest('tools/call', {
      name: 'create-livy-session',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        lakehouseId: config.lakehouseId,
        sessionConfig: {
          kind: 'pyspark',
          name: 'robust-data-session'
        }
      }
    });
    
    console.log('✅ Session creation response received');
    
    // Safely extract session info
    let sessionId;
    if (sessionResponse && sessionResponse.result && sessionResponse.result.content && sessionResponse.result.content[0]) {
      const sessionContent = sessionResponse.result.content[0].text;
      console.log('Session content:', sessionContent);
      
      const sessionData = JSON.parse(sessionContent);
      sessionId = sessionData.id;
      console.log(`🎯 Created Livy session with ID: ${sessionId}`);
    } else {
      throw new Error('Invalid session response format');
    }
    
    // Wait for session to be ready
    console.log('\n⏳ Waiting 20 seconds for session to start...');
    await new Promise(resolve => setTimeout(resolve, 20000));
    
    console.log('\n📊 Step 2: Executing data generation...');
    const dataResponse = await sendMcpRequest('tools/call', {
      name: 'execute-livy-statement',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        lakehouseId: config.lakehouseId,
        sessionId: sessionId,
        code: robustDataCode,
        kind: 'pyspark'
      }
    });
    
    console.log('✅ Data execution response received');
    
    // Safely extract execution info
    let statementId;
    if (dataResponse && dataResponse.result && dataResponse.result.content && dataResponse.result.content[0]) {
      const execContent = dataResponse.result.content[0].text;
      console.log('Execution content:', execContent);
      
      const execData = JSON.parse(execContent);
      statementId = execData.id;
      console.log(`🎯 Statement ID: ${statementId}, Status: ${execData.state}`);
    } else {
      throw new Error('Invalid execution response format');
    }
    
    // Wait for execution
    console.log('\n⏳ Waiting 30 seconds for code execution...');
    await new Promise(resolve => setTimeout(resolve, 30000));
    
    console.log('\n🔍 Step 3: Checking results...');
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
    
    console.log('✅ Result response received');
    
    if (resultResponse && resultResponse.result && resultResponse.result.content && resultResponse.result.content[0]) {
      const resultContent = resultResponse.result.content[0].text;
      console.log('Results:', resultContent);
      
      const resultData = JSON.parse(resultContent);
      console.log(`📊 Final Status: ${resultData.state}`);
      
      if (resultData.output && resultData.output.data) {
        console.log('📋 Execution Output:');
        console.log(resultData.output.data['text/plain']);
      }
    }
    
    console.log('\n🎉 Sample data generation process completed!');
    console.log(`✅ Lakehouse: MCPDemoLakehouse (${config.lakehouseId})`);
    console.log('📋 Tables should be created:');
    console.log('  - demo_sales (200 sales records)');
    console.log('  - sales_summary (aggregated by product and region)');
    console.log(`🌐 View in portal: https://msit.powerbi.com/groups/${config.workspaceId}`);
    
  } catch (error) {
    console.error('❌ Error in robust data generation:', error.message);
    console.error('Full error:', error);
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
    setTimeout(addRobustSampleData, 3000);
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
