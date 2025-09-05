const { spawn } = require('child_process');

// Configuration
const config = {
  workspaceId: process.env.FABRIC_DEFAULT_WORKSPACE_ID || 'bcb44215-0e69-46d3-aac9-fb92fadcd982',
  bearerToken: 'azure_cli',
  lakehouseId: '5e6b33fe-1f33-419a-a954-bce697ccfe61' // ID from the created lakehouse
};

console.log('📊 Adding Sample Data to Microsoft Fabric Lakehouse via MCP (Fixed)');
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

// Simple sample data generation code that doesn't require complex Spark configuration
const simpleDataCode = `
# Create sample data using basic Spark operations
print("🚀 Starting simple sample data generation...")

# Create sample sales data
from pyspark.sql import Row
from pyspark.sql.functions import *
import random

# Create simple sales data
sales_data = []
products = ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones']
regions = ['North', 'South', 'East', 'West']

for i in range(100):  # Smaller dataset for trial
    sales_data.append(Row(
        sale_id=i + 1,
        product=products[i % len(products)],
        quantity=random.randint(1, 5),
        unit_price=round(100 + (i * 10.5), 2),
        region=regions[i % len(regions)],
        total_amount=round((random.randint(1, 5) * (100 + (i * 10.5))), 2)
    ))

# Create DataFrame
sales_df = spark.createDataFrame(sales_data)

print(f"✅ Created sales dataset with {sales_df.count()} records")
sales_df.show(10)

# Save to lakehouse using delta format
sales_df.write.format("delta").mode('overwrite').saveAsTable('demo_sales')
print("✅ Saved demo_sales table to lakehouse")

# Create a simple summary
summary_df = sales_df.groupBy("product").agg(
    count("*").alias("total_sales"),
    sum("total_amount").alias("total_revenue"),
    avg("total_amount").alias("avg_sale")
)

print("📊 Sales Summary:")
summary_df.show()

# Save summary
summary_df.write.format("delta").mode('overwrite').saveAsTable('sales_summary')
print("✅ Saved sales_summary table to lakehouse")

print("🎉 Sample data generation completed successfully!")
`;

async function addSampleData() {
  try {
    console.log('\n🔄 Step 1: Creating Livy session with valid memory settings...');
    const sessionResponse = await sendMcpRequest('tools/call', {
      name: 'create-livy-session',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        lakehouseId: config.lakehouseId,
        sessionConfig: {
          kind: 'pyspark',
          name: 'sample-data-session',
          driverMemory: '28g',  // Using valid memory from error message
          executorMemory: '14g', // Using valid memory from error message
          numExecutors: 1
        }
      }
    });
    
    console.log('✅ Session creation response:');
    const sessionContent = sessionResponse.result.content[0].text;
    console.log(sessionContent);
    
    if (sessionContent.includes('Error')) {
      console.log('❌ Session creation failed. Trying with minimal configuration...');
      
      // Try with minimal configuration
      const minimalSessionResponse = await sendMcpRequest('tools/call', {
        name: 'create-livy-session',
        arguments: {
          bearerToken: config.bearerToken,
          workspaceId: config.workspaceId,
          lakehouseId: config.lakehouseId,
          sessionConfig: {
            kind: 'pyspark',
            name: 'minimal-session'
            // No memory settings - use defaults
          }
        }
      });
      
      console.log('✅ Minimal session response:');
      const minimalContent = minimalSessionResponse.result.content[0].text;
      console.log(minimalContent);
      
      if (minimalContent.includes('Error')) {
        throw new Error('Failed to create Livy session with both configurations');
      }
      
      // Parse session ID from minimal response
      const sessionData = JSON.parse(minimalContent);
      const sessionId = sessionData.id;
      console.log(`🎯 Created minimal Livy session with ID: ${sessionId}`);
      
      // Wait for session to be ready
      console.log('\n⏳ Waiting for session to start...');
      await new Promise(resolve => setTimeout(resolve, 15000));
      
      console.log('\n📊 Step 2: Executing sample data generation...');
      const dataResponse = await sendMcpRequest('tools/call', {
        name: 'execute-livy-statement',
        arguments: {
          bearerToken: config.bearerToken,
          workspaceId: config.workspaceId,
          lakehouseId: config.lakehouseId,
          sessionId: sessionId,
          code: simpleDataCode,
          kind: 'pyspark'
        }
      });
      
      console.log('✅ Data generation execution response:');
      console.log(JSON.stringify(dataResponse, null, 2));
      
      // Check execution status
      if (dataResponse.result && dataResponse.result.content) {
        const execContent = dataResponse.result.content[0].text;
        const execData = JSON.parse(execContent);
        const statementId = execData.id;
        
        console.log(`🎯 Statement ID: ${statementId}, Status: ${execData.state}`);
        
        // Wait for execution and check results
        console.log('\n⏳ Waiting for code execution...');
        await new Promise(resolve => setTimeout(resolve, 20000));
        
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
        
        console.log('✅ Execution results:');
        console.log(JSON.stringify(resultResponse, null, 2));
      }
      
      console.log('\n🎉 Sample data process completed!');
      console.log(`✅ Lakehouse: MCPDemoLakehouse (${config.lakehouseId})`);
      console.log('📋 Tables should be created:');
      console.log('  - demo_sales (100 sales records)');
      console.log('  - sales_summary (aggregated data)');
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
